package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"aurora-capcompute/internal/agent"
)

type Server struct {
	runtime   *agent.Runtime
	heartbeat time.Duration
	mux       *http.ServeMux
}

func New(runtime *agent.Runtime) *Server {
	server := &Server{
		runtime:   runtime,
		heartbeat: 15 * time.Second,
		mux:       http.NewServeMux(),
	}
	server.routes()
	return server
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /v1/threads", s.createThread)
	s.mux.HandleFunc("GET /v1/threads", s.listThreads)
	s.mux.HandleFunc("GET /v1/threads/{threadID}", s.getThread)
	s.mux.HandleFunc("POST /v1/threads/{threadID}/messages", s.createRun)
	s.mux.HandleFunc("GET /v1/threads/{threadID}/events", s.events)
	s.mux.HandleFunc("GET /v1/runs/{runID}", s.getRun)
	s.mux.HandleFunc("GET /v1/runs/{runID}/journal", s.getJournal)
	s.mux.HandleFunc("POST /v1/runs/{runID}/stop", s.stopRun)
	s.mux.HandleFunc("POST /v1/runs/{runID}/retry", s.retryRun)
}

func (s *Server) createThread(w http.ResponseWriter, request *http.Request) {
	var body struct {
		Manifest agent.Manifest `json:"manifest"`
	}
	if err := decodeJSON(request, &body); err != nil {
		writeError(w, err)
		return
	}
	thread, err := s.runtime.CreateThread(body.Manifest)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, thread)
}

func (s *Server) listThreads(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"threads": s.runtime.ListThreads()})
}

func (s *Server) getThread(w http.ResponseWriter, request *http.Request) {
	thread, err := s.runtime.GetThread(request.PathValue("threadID"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, thread)
}

func (s *Server) createRun(w http.ResponseWriter, request *http.Request) {
	var body struct {
		Content             string                   `json:"content"`
		CapabilityOverrides []agent.CapabilityConfig `json:"capability_overrides,omitempty"`
	}
	if err := decodeJSON(request, &body); err != nil {
		writeError(w, err)
		return
	}
	run, err := s.runtime.CreateRun(request.PathValue("threadID"), strings.TrimSpace(body.Content), body.CapabilityOverrides)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, run)
}

func (s *Server) getRun(w http.ResponseWriter, request *http.Request) {
	run, err := s.runtime.GetRun(request.PathValue("runID"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) getJournal(w http.ResponseWriter, request *http.Request) {
	journal, err := s.runtime.Journal(request.PathValue("runID"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": journal})
}

func (s *Server) stopRun(w http.ResponseWriter, request *http.Request) {
	run, err := s.runtime.Stop(request.PathValue("runID"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, run)
}

func (s *Server) retryRun(w http.ResponseWriter, request *http.Request) {
	var body struct {
		Mode                agent.RetryMode          `json:"mode"`
		CapabilityOverrides []agent.CapabilityConfig `json:"capability_overrides,omitempty"`
	}
	if err := decodeJSON(request, &body); err != nil {
		writeError(w, err)
		return
	}
	run, err := s.runtime.Retry(request.PathValue("runID"), body.Mode, body.CapabilityOverrides)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, run)
}

func (s *Server) events(w http.ResponseWriter, request *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, errorEnvelope{Error: "streaming_not_supported", Message: "response writer does not support streaming"})
		return
	}
	snapshot, events, unsubscribe, err := s.runtime.Subscribe(request.PathValue("threadID"))
	if err != nil {
		writeError(w, err)
		return
	}
	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	if err := writeEvent(w, snapshot); err != nil {
		return
	}
	flusher.Flush()

	heartbeat := time.NewTicker(s.heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-request.Context().Done():
			return
		case event := <-events:
			if err := writeEvent(w, event); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func writeEvent(w http.ResponseWriter, event agent.Event) error {
	raw, err := json.Marshal(event.Data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, raw)
	return err
}

func decodeJSON(request *http.Request, target any) error {
	defer request.Body.Close()
	decoder := json.NewDecoder(bufio.NewReader(io.LimitReader(request.Body, (1<<20)+1)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%w: decode JSON: %v", agent.ErrInvalid, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: request body must contain one JSON value", agent.ErrInvalid)
	}
	return nil
}

type errorEnvelope struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "internal_error"
	switch {
	case errors.Is(err, agent.ErrInvalid):
		status = http.StatusBadRequest
		code = "invalid_request"
	case errors.Is(err, agent.ErrNotFound):
		status = http.StatusNotFound
		code = "not_found"
	case errors.Is(err, agent.ErrConflict):
		status = http.StatusConflict
		code = "conflict"
	}
	writeJSON(w, status, errorEnvelope{Error: code, Message: err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func Shutdown(ctx context.Context, httpServer *http.Server, runtime *agent.Runtime) error {
	httpErr := httpServer.Shutdown(ctx)
	runtimeErr := runtime.Close(ctx)
	return errors.Join(httpErr, runtimeErr)
}
