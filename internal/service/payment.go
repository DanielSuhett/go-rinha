package service

import (
	"go-rinha/internal/config"
	"go-rinha/internal/repository"
	"go-rinha/internal/types"
	"go-rinha/pkg/health"
)

type PaymentService struct {
	circuitBreaker *health.Checker
	queueService   *QueueService
	repository     *repository.PaymentRepository
	config         *config.Config
}

func NewPaymentService(circuitBreaker *health.Checker, queueService *QueueService, repository *repository.PaymentRepository, config *config.Config) *PaymentService {
	queueService.SetHealthChecker(circuitBreaker)
	return &PaymentService{
		circuitBreaker: circuitBreaker,
		queueService:   queueService,
		repository:     repository,
		config:         config,
	}
}

func (p *PaymentService) ProcessPayment(data string) error {
	currentColor := p.circuitBreaker.GetCurrentColor()

	switch currentColor {
	case types.ColorRed:
		p.queueService.Requeue(data)
		return nil
	case types.ColorGreen:
		return p.repository.Send(types.ProcessorDefault, data, p.circuitBreaker, p.queueService)
	case types.ColorYellow:
		return p.repository.Send(types.ProcessorFallback, data, p.circuitBreaker, p.queueService)
	}
	
	return nil
}

func (p *PaymentService) ProcessPaymentBatch(items []string) error {
	currentColor := p.circuitBreaker.GetCurrentColor()

	switch currentColor {
	case types.ColorRed:
		for _, item := range items {
			p.queueService.Requeue(item)
		}
		return nil
	case types.ColorGreen:
		return p.repository.SendBatch(types.ProcessorDefault, items, p.circuitBreaker, p.queueService)
	case types.ColorYellow:
		return p.repository.SendBatch(types.ProcessorFallback, items, p.circuitBreaker, p.queueService)
	}
	
	return nil
}

func (p *PaymentService) GetPaymentSummary(fromTime, toTime *int64) (*types.PaymentSummaryResponse, error) {
	return p.repository.FindAll(fromTime, toTime)
}