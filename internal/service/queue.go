package service

import (
	"context"
	"encoding/json"
	"go-rinha/internal/config"
	"go-rinha/internal/types"
	"log"
	"sync"
	"time"
)

type QueueService struct {
	fastQueue        *FastQueue
	duplicateTracker sync.Map
	config           *config.Config
	ctx              context.Context
	cancel           context.CancelFunc
	paymentProcessor func(string) error
	isProcessing     bool
	healthChecker    HealthChecker
}

type HealthChecker interface {
	GetCurrentColor() types.CircuitBreakerColor
}

func NewQueueService(config *config.Config) *QueueService {
	ctx, cancel := context.WithCancel(context.Background())

	queueSize := 1024 * 16
	q := &QueueService{
		fastQueue: NewFastQueue(queueSize),
		config:    config,
		ctx:       ctx,
		cancel:    cancel,
	}

	q.startDuplicateCleanup()
	return q
}

func (q *QueueService) SetPaymentProcessor(processor func(string) error) {
	q.paymentProcessor = processor
}

func (q *QueueService) SetHealthChecker(healthChecker HealthChecker) {
	q.healthChecker = healthChecker
}


func (q *QueueService) Add(item string) {
	var payment types.PaymentRequest
	if err := json.Unmarshal([]byte(item), &payment); err != nil {
		log.Printf("Failed to parse payment for duplicate check: %v", err)
		return
	}

	if _, exists := q.duplicateTracker.LoadOrStore(payment.CorrelationID, time.Now().Unix()); exists {
		log.Printf("Duplicate payment ignored: %s", payment.CorrelationID)
		return
	}

	if !q.fastQueue.Push([]byte(item)) {
		log.Printf("Failed to push item to queue: queue full")
	}
}

func (q *QueueService) Requeue(item string) {
	if !q.fastQueue.PushFront([]byte(item)) {
		log.Printf("Failed to requeue item: queue full")
	}
}

func (q *QueueService) Start() {
	q.isProcessing = true
	go q.startProcessing()
}

func (q *QueueService) Stop() {
	q.isProcessing = false
	q.cancel()
}

func (q *QueueService) startProcessing() {
	for q.isProcessing {
		select {
		case <-q.ctx.Done():
			return
		default:
			q.processBatch()
		}
	}
}

func (q *QueueService) processBatch() {
	if q.healthChecker != nil {
		currentColor := q.healthChecker.GetCurrentColor()
		if currentColor == types.ColorRed {
			time.Sleep(time.Duration(q.config.PollingInterval) * time.Millisecond)
			return
		}
	}

	batchBytes := q.fastQueue.PopBatch(q.config.BatchSize)
	if len(batchBytes) == 0 {
		time.Sleep(time.Duration(q.config.PollingInterval) * time.Millisecond)
		return
	}

	for _, item := range batchBytes {
		if q.paymentProcessor != nil {
			if err := q.paymentProcessor(string(item)); err != nil {
				log.Printf("Payment processing error: %v", err)
				q.Requeue(string(item))
			}
		}
	}
}


func (q *QueueService) startDuplicateCleanup() {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-q.ctx.Done():
				return
			case <-ticker.C:
				now := time.Now().Unix()
				cutoff := now - 300

				q.duplicateTracker.Range(func(key, value any) bool {
					if timestamp, ok := value.(int64); ok && timestamp < cutoff {
						q.duplicateTracker.Delete(key)
					}
					return true
				})
			}
		}
	}()
}
