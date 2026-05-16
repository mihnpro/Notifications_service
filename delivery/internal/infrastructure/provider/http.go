package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	domainprovider "github.com/notifications/delivery/internal/domain/provider"
)

// HTTPAdapter calls an external provider HTTP service (e.g. provider_mock).
// POST /send → 200 success | 422 transient error | 400 permanent error.
type HTTPAdapter struct {
	baseURL string
	client  *http.Client
}

func NewHTTPAdapter(baseURL string) *HTTPAdapter {
	return &HTTPAdapter{
		baseURL: baseURL,
		client:  &http.Client{}, // deadline comes from the caller's context
	}
}

type httpSendRequest struct {
	TaskID         string          `json:"task_id"`
	Recipient      string          `json:"recipient"`
	Channel        string          `json:"channel"`
	Message        json.RawMessage `json:"message"`
	IdempotencyKey string          `json:"idempotency_key"`
}

type httpSendResponse struct {
	ProviderRequestID string `json:"provider_request_id"`
	ErrorType         string `json:"error_type"`
	ErrorCode         string `json:"error_code"`
	Message           string `json:"message"`
}

func (a *HTTPAdapter) Send(ctx context.Context, p domainprovider.Payload) (domainprovider.Result, error) {
	body, err := json.Marshal(httpSendRequest{
		TaskID:         p.TaskID.String(),
		Recipient:      p.RecipientAddr,
		Channel:        p.ChannelCode,
		Message:        p.Message,
		IdempotencyKey: p.IdempotencyKey,
	})
	if err != nil {
		return domainprovider.Result{}, fmt.Errorf("marshal provider request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/send", bytes.NewReader(body))
	if err != nil {
		return domainprovider.Result{}, fmt.Errorf("build provider request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		// Network / timeout errors — treat as transient so the worker retries.
		return domainprovider.Result{}, &domainprovider.Error{
			Type:    domainprovider.ErrorTypeTransient,
			Code:    "PROVIDER_UNREACHABLE",
			Message: err.Error(),
		}
	}
	defer resp.Body.Close()

	var res httpSendResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return domainprovider.Result{}, fmt.Errorf("decode provider response: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return domainprovider.Result{ProviderRequestID: res.ProviderRequestID}, nil

	case http.StatusUnprocessableEntity: // 422 — transient
		return domainprovider.Result{}, &domainprovider.Error{
			Type:    domainprovider.ErrorTypeTransient,
			Code:    res.ErrorCode,
			Message: res.Message,
		}

	case http.StatusBadRequest: // 400 — permanent
		return domainprovider.Result{}, &domainprovider.Error{
			Type:    domainprovider.ErrorTypePermanent,
			Code:    res.ErrorCode,
			Message: res.Message,
		}

	default:
		return domainprovider.Result{}, &domainprovider.Error{
			Type:    domainprovider.ErrorTypeTransient,
			Code:    "HTTP_ERROR",
			Message: fmt.Sprintf("unexpected provider status %d: %s", resp.StatusCode, res.Message),
		}
	}
}
