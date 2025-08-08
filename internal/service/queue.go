package service

import (
	"context"
	"go-rinha/internal/config"
	"go-rinha/internal/types"
	"log"
	"strings"
	"sync"
	"time"
)

type QueueService struct {
	fastQueue             *FastQueue
	duplicateTracker      sync.Map
	config                *config.Config
	ctx                   context.Context
	cancel                context.CancelFunc
	paymentProcessor      func(string) error
	paymentBatchProcessor func([]string) error
	isProcessing          bool
	healthChecker         HealthChecker
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

func (q *QueueService) SetPaymentBatchProcessor(processor func([]string) error) {
	q.paymentBatchProcessor = processor
}

func (q *QueueService) SetHealthChecker(healthChecker HealthChecker) {
	q.healthChecker = healthChecker
}

func (q *QueueService) Add(item []byte) {
	start := time.Now()
	if !q.fastQueue.Push(string(item)) {
		log.Printf("queue full")
	}
	duration := time.Since(start)
	if duration >= time.Millisecond {
		log.Printf("PERF: FastQueue.Push took %v", duration)
	}
}

func (q *QueueService) Requeue(item string) {
	correlationID := extractCorrelationID(item)
	if _, exists := q.duplicateTracker.LoadOrStore(correlationID, time.Now().Unix()); exists {
		return
	}

	if !q.fastQueue.PushFront(item) {
		log.Printf("queue full")
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

	start := time.Now()
	batch := q.fastQueue.PopBatch(q.config.BatchSize)
	popDuration := time.Since(start)

	if len(batch) == 0 {
		time.Sleep(time.Duration(q.config.PollingInterval) * time.Millisecond)
		return
	}

	if popDuration >= time.Millisecond {
		log.Printf("PERF: PopBatch(%d items) took %v", len(batch), popDuration)
	}

	go q.processBatchAsync(batch)
}

func (q *QueueService) processBatchAsync(batch []string) {
	batchStart := time.Now()

	if q.paymentBatchProcessor != nil {
		if err := q.paymentBatchProcessor(batch); err != nil {
			log.Printf("Batch payment processing error: %v", err)
		}
	} else if q.paymentProcessor != nil {
		var wg sync.WaitGroup
		for _, item := range batch {
			wg.Add(1)
			go func(item string) {
				defer wg.Done()

				batchDuration := time.Since(batchStart)
				if batchDuration >= time.Millisecond {
					log.Printf("PERF: ProcessBatchAsync(%d items) took %v", len(batch), batchDuration)
				}
				if err := q.paymentProcessor(item); err != nil {
					log.Printf("Payment processing error: %v", err)
					q.Requeue(item)
				}
			}(item)
		}
		wg.Wait()
	}
}

func extractCorrelationID(jsonStr string) string {
	const key = `"correlationId":"`
	start := strings.Index(jsonStr, key)
	if start == -1 {
		return ""
	}
	start += len(key)
	end := strings.Index(jsonStr[start:], `"`)
	if end == -1 {
		return ""
	}
	return jsonStr[start : start+end]
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
