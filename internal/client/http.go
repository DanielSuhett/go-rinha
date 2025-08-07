package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go-rinha/internal/types"
	"io"
	"log"
	"net/http"
	"time"
)

type HTTPClient struct {
	client *http.Client
}

func NewHTTPClient() *HTTPClient {
	transport := &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   50,
		MaxConnsPerHost:       100,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   2 * time.Second,
		ResponseHeaderTimeout: 2 * time.Second,
		DisableKeepAlives:     false,
		DisableCompression:    true,
		ForceAttemptHTTP2:     false,
	}

	return &HTTPClient{
		client: &http.Client{
			Transport: transport,
			Timeout:   3 * time.Second,
		},
	}
}

func (h *HTTPClient) PostPayment(url string, payment *types.PaymentRequest) (int, error) {
	jsonData, err := json.Marshal(payment)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal payment: %w", err)
	}

	resp, err := h.client.Post(url, "application/json", bytes.NewReader(jsonData))
	if err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, nil
	}

	if resp.StatusCode >= 400 {
		log.Printf("Payment processor error %d: %s", resp.StatusCode, string(body))
	}

	return resp.StatusCode, nil
}

func (h *HTTPClient) GetHealth(url string, timeout time.Duration) (int, []byte, time.Duration, error) {
	oldTimeout := h.client.Timeout
	h.client.Timeout = timeout
	defer func() {
		h.client.Timeout = oldTimeout
	}()

	start := time.Now()
	resp, err := h.client.Get(url)
	duration := time.Since(start)

	if err != nil {
		return 0, nil, duration, fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, duration, fmt.Errorf("failed to read response body: %w", err)
	}

	return resp.StatusCode, body, duration, nil
}

func (h *HTTPClient) Close() {
	if transport, ok := h.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
}
