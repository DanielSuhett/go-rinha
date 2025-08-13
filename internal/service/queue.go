package service

import (
	"context"
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
	paymentProcessor func([]byte) error
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

func (q *QueueService) SetPaymentProcessor(processor func([]byte) error) {
	q.paymentProcessor = processor
}

func (q *QueueService) SetHealthChecker(healthChecker HealthChecker) {
	q.healthChecker = healthChecker
}

func (q *QueueService) Add(item []byte) {
	if !q.fastQueue.Push(item) {
		log.Printf("Failed to push item to queue: queue full")
	}
}

func (q *QueueService) Requeue(item []byte) {
	if !q.fastQueue.PushFront(item) {
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
	currentColor := q.healthChecker.GetCurrentColor()
	if currentColor == types.ColorRed {
		time.Sleep(time.Duration(q.config.PollingInterval) * time.Millisecond)
		return
	}

	batchBytes := q.fastQueue.PopBatch(q.config.BatchSize)
	if len(batchBytes) == 0 {
		return
	}

	for i, item := range batchBytes {
		if q.paymentProcessor != nil {
			if err := q.paymentProcessor(item); err != nil {
				log.Printf("Payment processing error: %v", err)
				q.Requeue(item)
			}
		}
		if i < len(batchBytes)-1 && len(batchBytes) > 1 {
			time.Sleep(10 * time.Microsecond)
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
