package types

import "time"

type Processor string

const (
	ProcessorDefault  Processor = "default"
	ProcessorFallback Processor = "fallback"
)

type CircuitBreakerColor string

const (
	ColorGreen  CircuitBreakerColor = "GREEN"
	ColorYellow CircuitBreakerColor = "YELLOW"
	ColorRed    CircuitBreakerColor = "RED"
)

type PaymentRequest struct {
	CorrelationID string  `json:"correlationId" validate:"required"`
	Amount        float64 `json:"amount" validate:"required,gt=0"`
	RequestedAt   string  `json:"requestedAt,omitempty"`
}

type PaymentSummary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type PaymentSummaryResponse struct {
	Default  PaymentSummary `json:"default"`
	Fallback PaymentSummary `json:"fallback"`
}

type ProcessorHealth struct {
	MinResponseTime int  `json:"minResponseTime"`
	Failing         bool `json:"failing"`
}

type ProcessorHealthState struct {
	DefaultHealthy  bool
	FallbackHealthy bool
	LastCheck       time.Time
}

type HealthCheckConfig struct {
	ProcessorUrls    ProcessorUrls `json:"processorUrls"`
	HealthTimeout    int           `json:"healthTimeout"`
	HealthInterval   int           `json:"healthInterval"`
	LatencyThreshold int           `json:"latencyDiffToUseFallback"`
	PollingInterval  int           `json:"pollingInterval"`
}

type ProcessorUrls struct {
	Default  string `json:"default"`
	Fallback string `json:"fallback"`
}

type WorkerMessage struct {
	Type      string              `json:"type"`
	Color     CircuitBreakerColor `json:"color,omitempty"`
	Processor Processor           `json:"processor,omitempty"`
	Timestamp int64               `json:"timestamp,omitempty"`
	Config    *HealthCheckConfig  `json:"config,omitempty"`
}

const (
	ChannelCircuitColor      = "rinha:circuit:color"
	MessageTypeColorUpdate   = "COLOR_UPDATE"
	MessageTypeWorkerReady   = "WORKER_READY"
	MessageTypeConfigUpdate  = "CONFIG_UPDATE"
	MessageTypeSignalFailure = "SIGNAL_FAILURE"
	MessageTypeShutdown      = "SHUTDOWN"
)