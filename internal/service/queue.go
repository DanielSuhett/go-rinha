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
	queue                 chan string
	priorityQueue         chan string
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

	q := &QueueService{
		queue:         make(chan string, 17000),
		priorityQueue: make(chan string, 1000),
		config:        config,
		ctx:           ctx,
		cancel:        cancel,
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
	select {
	case q.queue <- string(item):
	default:
		log.Printf("queue full")
	}
}

func (q *QueueService) Requeue(item string) {
	select {
	case q.priorityQueue <- item:
	default:
		select {
		case q.queue <- item:
		default:
			log.Printf("queue full")
		}
	}
}

func (q *QueueService) Start() {
	q.isProcessing = true
	go q.startProcessing()
}

func (q *QueueService) Stop() {
	q.isProcessing = false
	q.cancel()
	close(q.queue)
	close(q.priorityQueue)
}

func (q *QueueService) Size() int {
	return len(q.queue) + len(q.priorityQueue)
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
	batch := q.popBatch(q.config.BatchSize)
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

func (q *QueueService) popBatch(maxCount int) []string {
	if maxCount <= 0 {
		return nil
	}

	batch := make([]string, 0, maxCount)
	timeout := time.After(10 * time.Millisecond)

	for len(batch) < maxCount {
		select {
		case item := <-q.priorityQueue:
			batch = append(batch, item)
		case item := <-q.queue:
			batch = append(batch, item)
		case <-timeout:
			return batch
		default:
			if len(batch) > 0 {
				return batch
			}
			return nil
		}
	}

	return batch
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
