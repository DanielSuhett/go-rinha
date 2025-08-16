package types


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

