package service

import (
	"context"
	"go-rinha/internal/config"
	"go-rinha/internal/types"
	"log"
	"sync"
	"sync/atomic"
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
	isProcessing          int32
	healthChecker         HealthChecker
	workers               sync.WaitGroup
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
	start := time.Now()
	itemStr := string(item)

	select {
	case q.queue <- itemStr:
	default:
		log.Printf("queue full")
	}

	addDuration := time.Since(start)
	if addDuration >= time.Millisecond {
		log.Printf("PERF: Add() took %v", addDuration)
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
	atomic.StoreInt32(&q.isProcessing, 1)

	for i := 0; i < q.config.WorkerCount; i++ {
		q.workers.Add(1)
		go q.startWorker(i)
	}
}

func (q *QueueService) Stop() {
	atomic.StoreInt32(&q.isProcessing, 0)
	q.cancel()
	close(q.queue)
	close(q.priorityQueue)
	q.workers.Wait()
}

func (q *QueueService) Size() int {
	return len(q.queue) + len(q.priorityQueue)
}

func (q *QueueService) startWorker(_ int) {
	defer q.workers.Done()

	for atomic.LoadInt32(&q.isProcessing) == 1 {
		select {
		case <-q.ctx.Done():
			return
		default:
			if q.healthChecker != nil && q.healthChecker.GetCurrentColor() == types.ColorRed {
				time.Sleep(time.Duration(q.config.PollingInterval) * time.Millisecond)
				continue
			}

			batch := q.popBatch(q.config.BatchSize)

			if len(batch) == 0 {
				time.Sleep(time.Millisecond)
				continue
			}

			q.processBatch(batch)
		}
	}
}

func (q *QueueService) popBatch(maxCount int) []string {
	if maxCount <= 0 {
		return nil
	}

	batch := make([]string, 0, maxCount)
	timeout := time.After(5 * time.Millisecond)

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

func (q *QueueService) processBatch(batch []string) {
	if q.healthChecker != nil {
		currentColor := q.healthChecker.GetCurrentColor()
		if currentColor == types.ColorRed {
			for _, item := range batch {
				q.Requeue(item)
			}
			return
		}
	}

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
