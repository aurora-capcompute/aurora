package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"aurora-capcompute/internal/agent"
	"aurora-capcompute/internal/llm"
	auroraserver "aurora-capcompute/internal/server"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	llmClient, err := llmFromEnv()
	if err != nil {
		return err
	}
	runtime, err := agent.NewRuntime(ctx, agent.Config{
		WasmPath: envDefault("AURORA_GUEST_WASM", "guest/agent.wasm"),
		LLM:      llmClient,
	})
	if err != nil {
		return fmt.Errorf("create agent runtime: %w", err)
	}

	address := envDefault("AURORA_SERVER_ADDR", "127.0.0.1:8080")
	httpServer := &http.Server{
		Addr:              address,
		Handler:           auroraserver.New(runtime).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	errs := make(chan error, 1)
	go func() {
		log.Printf("Aurora server listening on http://%s", address)
		errs <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			_ = runtime.Close(context.Background())
			return err
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return auroraserver.Shutdown(shutdownCtx, httpServer, runtime)
	}
}

func llmFromEnv() (llm.Client, error) {
	switch strings.ToLower(envDefault("AURORA_LLM", "fake")) {
	case "fake":
		return llm.NewFakeClient(os.Getenv("AURORA_FAKE_READ_URL")), nil
	case "openai":
		return llm.NewOpenAIClient(llm.OpenAIConfigFromEnv())
	default:
		return nil, fmt.Errorf("unsupported AURORA_LLM: %s", os.Getenv("AURORA_LLM"))
	}
}

func envDefault(name string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
