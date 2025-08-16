package client

import (
	"fmt"
	"net/url"
	"time"

	"github.com/valyala/fasthttp"
)

type HTTPClient struct {
	clients map[string]*fasthttp.HostClient
}

func NewHTTPClient() *HTTPClient {
	return &HTTPClient{
		clients: make(map[string]*fasthttp.HostClient),
	}
}

func (h *HTTPClient) getOrCreateClient(targetURL string) *fasthttp.HostClient {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return nil
	}
	
	host := parsedURL.Host
	if client, exists := h.clients[host]; exists {
		return client
	}

	client := &fasthttp.HostClient{
		Addr:                          host,
		MaxConns:                      50,
		MaxIdleConnDuration:           30 * time.Second,
		MaxConnDuration:               0,
		MaxIdemponentCallAttempts:     1,
		ReadTimeout:                   1 * time.Second,
		WriteTimeout:                  1 * time.Second,
		MaxResponseBodySize:           4096,
		DisableHeaderNamesNormalizing: true,
		DisablePathNormalizing:        true,
		NoDefaultUserAgentHeader:      true,
	}

	h.clients[host] = client
	return client
}

func (h *HTTPClient) PostPayment(targetURL string, jsonData []byte) (int, error) {
	client := h.getOrCreateClient(targetURL)
	if client == nil {
		return 0, fmt.Errorf("invalid URL: %s", targetURL)
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	parsedURL, _ := url.Parse(targetURL)
	req.SetRequestURI(parsedURL.Path)
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")
	req.Header.SetHost(parsedURL.Host)
	req.SetBody(jsonData)

	err := client.Do(req, resp)
	if err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}

	return resp.StatusCode(), nil
}

func (h *HTTPClient) GetHealth(targetURL string, timeout time.Duration) (int, []byte, error) {
	client := h.getOrCreateClient(targetURL)
	if client == nil {
		return 0, nil, fmt.Errorf("invalid URL: %s", targetURL)
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	parsedURL, _ := url.Parse(targetURL)
	req.SetRequestURI(parsedURL.Path)
	req.Header.SetMethod("GET")
	req.Header.SetHost(parsedURL.Host)

	err := client.DoTimeout(req, resp, timeout)
	if err != nil {
		return 0, nil, fmt.Errorf("health check failed: %w", err)
	}

	body := make([]byte, len(resp.Body()))
	copy(body, resp.Body())

	return resp.StatusCode(), body, nil
}

func (h *HTTPClient) Close() {
	for _, client := range h.clients {
		client.CloseIdleConnections()
	}
}
