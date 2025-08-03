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

	workerChan  chan string
	workerWg    sync.WaitGroup
	workerCount int

	requeueChan  chan string
	requeueWg    sync.WaitGroup
	requeueCount int
}

type HealthChecker interface {
	GetCurrentColor() types.CircuitBreakerColor
}

func NewQueueService(config *config.Config) *QueueService {
	ctx, cancel := context.WithCancel(context.Background())

	queueSize := 1024 * 16
	q := &QueueService{
		fastQueue:    NewFastQueue(queueSize),
		config:       config,
		ctx:          ctx,
		cancel:       cancel,
		workerChan:   make(chan string, config.WorkerChanBuffer),
		workerCount:  config.WorkerPoolSize,
		requeueChan:  make(chan string, config.RequeueChanBuffer),
		requeueCount: config.RequeuePoolSize,
	}

	q.startWorkers()
	q.startRequeueWorkers()
	q.startDuplicateCleanup()
	return q
}

func (q *QueueService) SetPaymentProcessor(processor func(string) error) {
	q.paymentProcessor = processor
}

func (q *QueueService) SetHealthChecker(healthChecker HealthChecker) {
	q.healthChecker = healthChecker
}

func (q *QueueService) startWorkers() {
	for i := 0; i < q.workerCount; i++ {
		q.workerWg.Add(1)
		go q.worker()
	}
}

func (q *QueueService) startRequeueWorkers() {
	for i := 0; i < q.requeueCount; i++ {
		q.requeueWg.Add(1)
		go q.requeueWorker()
	}
}

func (q *QueueService) worker() {
	defer q.workerWg.Done()

	for {
		select {
		case <-q.ctx.Done():
			return
		case item := <-q.workerChan:
			if !q.fastQueue.Push([]byte(item)) {
				log.Printf("Failed to push item to queue: queue full")
			}
		}
	}
}

func (q *QueueService) requeueWorker() {
	defer q.requeueWg.Done()

	for {
		select {
		case <-q.ctx.Done():
			return
		case item := <-q.requeueChan:
			if !q.fastQueue.PushFront([]byte(item)) {
				log.Printf("Failed to requeue item: queue full")
			}
		}
	}
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

	select {
	case q.workerChan <- item:
	case <-q.ctx.Done():
		log.Printf("Queue service stopped, cannot add item")
	default:
		log.Printf("Worker channel full, attempting direct queue push")
		if !q.fastQueue.Push([]byte(item)) {
			log.Printf("Failed to push item to queue: queue full")
		}
	}
}

func (q *QueueService) Requeue(item string) {
	select {
	case q.requeueChan <- item:
	case <-q.ctx.Done():
		log.Printf("Queue service stopped, cannot requeue item")
	default:
		log.Printf("Requeue channel full, attempting direct queue push")
		if !q.fastQueue.PushFront([]byte(item)) {
			log.Printf("Failed to requeue item: queue full")
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

	close(q.workerChan)
	q.workerWg.Wait()

	close(q.requeueChan)
	q.requeueWg.Wait()
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

	batch := make([]string, len(batchBytes))
	for i, item := range batchBytes {
		batch[i] = string(item)
	}

	q.processItems(batch)
}

func (q *QueueService) processItems(items []string) {
	for _, item := range items {
		if q.paymentProcessor != nil {
			if err := q.paymentProcessor(item); err != nil {
				log.Printf("Payment processing error: %v", err)
				q.Requeue(item)
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

				q.duplicateTracker.Range(func(key, value interface{}) bool {
					if timestamp, ok := value.(int64); ok && timestamp < cutoff {
						q.duplicateTracker.Delete(key)
					}
					return true
				})
			}
		}
	}()
}
