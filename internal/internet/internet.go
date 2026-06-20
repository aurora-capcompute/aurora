package internet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultTimeout          = 10 * time.Second
	DefaultMaxResponseBytes = 64 * 1024
)

type ReadRequest struct {
	Method string `json:"method"`
	URL    string `json:"url"`
}

type ReadResponse struct {
	URL         string `json:"url"`
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	Body        string `json:"body"`
}

type Policy struct {
	rules []rule
}

type rule struct {
	method string
	scheme string
	host   string
	all    bool
}

func ParseAllowlist(raw string) (Policy, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Policy{}, nil
	}
	entries := strings.Split(raw, ",")
	rules := make([]rule, 0, len(entries))
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		method, origin, ok := strings.Cut(entry, ":")
		if !ok {
			return Policy{}, fmt.Errorf("invalid allowlist entry %q", entry)
		}
		method = strings.ToUpper(strings.TrimSpace(method))
		if method == "" {
			return Policy{}, fmt.Errorf("allowlist entry %q has empty method", entry)
		}
		origin = strings.TrimSpace(origin)
		if origin == "*" {
			rules = append(rules, rule{method: method, all: true})
			continue
		}
		parsed, err := url.Parse(origin)
		if err != nil {
			return Policy{}, fmt.Errorf("parse allowlist entry %q: %w", entry, err)
		}
		if err := validateScheme(parsed.Scheme); err != nil {
			return Policy{}, fmt.Errorf("allowlist entry %q: %w", entry, err)
		}
		if parsed.Host == "" {
			return Policy{}, fmt.Errorf("allowlist entry %q has empty host", entry)
		}
		if parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
			return Policy{}, fmt.Errorf("allowlist entry %q must be an origin", entry)
		}
		rules = append(rules, rule{
			method: method,
			scheme: strings.ToLower(parsed.Scheme),
			host:   strings.ToLower(parsed.Host),
		})
	}
	return Policy{rules: rules}, nil
}

func (p Policy) Check(method string, target *url.URL) error {
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		return errors.New("method is required")
	}
	if target == nil {
		return errors.New("url is required")
	}
	if err := validateScheme(target.Scheme); err != nil {
		return err
	}
	if target.Host == "" {
		return errors.New("url host is required")
	}
	if target.User != nil {
		return errors.New("url credentials are not allowed")
	}
	scheme := strings.ToLower(target.Scheme)
	host := strings.ToLower(target.Host)
	for _, rule := range p.rules {
		if rule.method == method && (rule.all || rule.scheme == scheme && rule.host == host) {
			return nil
		}
	}
	return fmt.Errorf("%s %s://%s is not allowlisted", method, scheme, host)
}

func (p Policy) Allows(method string, rawURL string) error {
	target, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	return p.Check(method, target)
}

type Client struct {
	Policy     Policy
	HTTPClient *http.Client
	Timeout    time.Duration
	MaxBytes   int64
}

func NewClient(policy Policy) *Client {
	return NewConfiguredClient(policy, DefaultTimeout, DefaultMaxResponseBytes)
}

func NewConfiguredClient(policy Policy, timeout time.Duration, maxBytes int64) *Client {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxResponseBytes
	}
	client := &Client{
		Policy:   policy,
		Timeout:  timeout,
		MaxBytes: maxBytes,
	}
	client.HTTPClient = &http.Client{
		Timeout: client.Timeout,
		CheckRedirect: func(req *http.Request, _ []*http.Request) error {
			return client.Policy.Check(req.Method, req.URL)
		},
	}
	return client
}

func (c *Client) Read(ctx context.Context, request ReadRequest) (ReadResponse, error) {
	method := strings.ToUpper(strings.TrimSpace(request.Method))
	if method != http.MethodGet {
		return ReadResponse{}, fmt.Errorf("unsupported method %q: v0 implements GET only", request.Method)
	}

	target, err := url.Parse(strings.TrimSpace(request.URL))
	if err != nil {
		return ReadResponse{}, fmt.Errorf("parse url: %w", err)
	}
	if err := c.Policy.Check(method, target); err != nil {
		return ReadResponse{}, err
	}

	httpRequest, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return ReadResponse{}, fmt.Errorf("create request: %w", err)
	}
	httpRequest.Header.Set("Accept", "text/plain,text/html,application/json,application/xml,text/*;q=0.9,*/*;q=0.1")

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = NewClient(c.Policy).HTTPClient
	}
	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		return ReadResponse{}, err
	}
	defer httpResponse.Body.Close()

	contentType := httpResponse.Header.Get("Content-Type")
	if !isTextualContentType(contentType) {
		return ReadResponse{}, fmt.Errorf("content type %q is not textual", contentType)
	}

	body, err := readBounded(httpResponse.Body, c.maxBytes())
	if err != nil {
		return ReadResponse{}, fmt.Errorf("read response body: %w", err)
	}
	return ReadResponse{
		URL:         httpResponse.Request.URL.String(),
		Status:      httpResponse.StatusCode,
		ContentType: contentType,
		Body:        body,
	}, nil
}

func (c *Client) maxBytes() int64 {
	if c == nil || c.MaxBytes <= 0 {
		return DefaultMaxResponseBytes
	}
	return c.MaxBytes
}

func validateScheme(scheme string) error {
	switch strings.ToLower(scheme) {
	case "http", "https":
		return nil
	default:
		return fmt.Errorf("scheme %q is not allowed", scheme)
	}
}

func isTextualContentType(contentType string) bool {
	if strings.TrimSpace(contentType) == "" {
		return true
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		mediaType = contentType
	}
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	if strings.HasPrefix(mediaType, "text/") {
		return true
	}
	switch mediaType {
	case "application/json", "application/ld+json", "application/xml", "application/xhtml+xml",
		"application/javascript", "application/x-javascript", "application/x-www-form-urlencoded":
		return true
	default:
		return strings.HasSuffix(mediaType, "+json") || strings.HasSuffix(mediaType, "+xml")
	}
}

func readBounded(reader io.Reader, maxBytes int64) (string, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return "", err
	}
	if int64(len(data)) > maxBytes {
		data = data[:maxBytes]
	}
	return string(data), nil
}
