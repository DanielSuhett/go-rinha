package client

import (
	"encoding/json"
	"fmt"
	"go-rinha/internal/types"
	"time"

	"github.com/valyala/fasthttp"
)

type HTTPClient struct {
	client *fasthttp.Client
}

func NewHTTPClient() *HTTPClient {
	return &HTTPClient{
		client: &fasthttp.Client{
			ReadTimeout:         500 * time.Millisecond,
			WriteTimeout:        500 * time.Millisecond,
			MaxConnsPerHost:     50,
			MaxIdleConnDuration: 5 * time.Second,
		},
	}
}

func (h *HTTPClient) PostPayment(url string, payment *types.PaymentRequest) (int, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	jsonData, err := json.Marshal(payment)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal payment: %w", err)
	}

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")
	req.SetBody(jsonData)

	err = h.client.DoTimeout(req, resp, 2*time.Second)
	if err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}

	return resp.StatusCode(), nil
}

func (h *HTTPClient) GetHealth(url string, timeout time.Duration) (int, []byte, time.Duration, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodGet)

	start := time.Now()
	err := h.client.DoTimeout(req, resp, timeout)
	duration := time.Since(start)

	if err != nil {
		return 0, nil, duration, fmt.Errorf("health check failed: %w", err)
	}

	body := make([]byte, len(resp.Body()))
	copy(body, resp.Body())

	return resp.StatusCode(), body, duration, nil
}

func (h *HTTPClient) Close() {
}
