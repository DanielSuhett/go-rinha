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
	queue                 []string
	priorityQueue         []string
	queueMutex            sync.RWMutex
	priorityMutex         sync.RWMutex
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
		queue:         make([]string, 0, 17000),
		priorityQueue: make([]string, 0, 1000),
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
	itemStr := string(item)
	
	q.queueMutex.Lock()
	q.queue = append(q.queue, itemStr)
	q.queueMutex.Unlock()
}

func (q *QueueService) Requeue(item string) {
	q.priorityMutex.Lock()
	if len(q.priorityQueue) < cap(q.priorityQueue) {
		q.priorityQueue = append(q.priorityQueue, item)
		q.priorityMutex.Unlock()
		return
	}
	q.priorityMutex.Unlock()

	q.queueMutex.Lock()
	q.queue = append(q.queue, item)
	q.queueMutex.Unlock()
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
	q.workers.Wait()
}

func (q *QueueService) Size() int {
	q.priorityMutex.RLock()
	prioritySize := len(q.priorityQueue)
	q.priorityMutex.RUnlock()
	
	q.queueMutex.RLock()
	queueSize := len(q.queue)
	q.queueMutex.RUnlock()
	
	return prioritySize + queueSize
}

func (q *QueueService) startWorker(_ int) {
	defer q.workers.Done()
	
	backoffTime := 500 * time.Microsecond
	maxBackoff := 50 * time.Millisecond
	emptyCount := 0
	
	for atomic.LoadInt32(&q.isProcessing) == 1 {
		select {
		case <-q.ctx.Done():
			return
		default:
			if q.healthChecker != nil && q.healthChecker.GetCurrentColor() == types.ColorRed {
				time.Sleep(time.Duration(q.config.PollingInterval) * time.Millisecond)
				emptyCount = 0
				backoffTime = 500 * time.Microsecond
				continue
			}
			
			batch := q.popBatch(q.config.BatchSize)
			
			if len(batch) == 0 {
				emptyCount++
				if emptyCount > 5 {
					time.Sleep(backoffTime)
					if backoffTime < maxBackoff {
						backoffTime = min(backoffTime*2, maxBackoff)
					}
				} else {
					time.Sleep(100 * time.Microsecond)
				}
				continue
			}
			
			emptyCount = 0
			backoffTime = 500 * time.Microsecond
			q.processBatch(batch)
		}
	}
}

func (q *QueueService) popBatch(maxCount int) []string {
	if maxCount <= 0 {
		return nil
	}

	batch := make([]string, 0, maxCount)

	q.priorityMutex.Lock()
	priorityLen := len(q.priorityQueue)
	if priorityLen > 0 {
		takeFromPriority := min(priorityLen, maxCount)
		
		batch = append(batch, q.priorityQueue[:takeFromPriority]...)
		copy(q.priorityQueue, q.priorityQueue[takeFromPriority:])
		q.priorityQueue = q.priorityQueue[:priorityLen-takeFromPriority]
		maxCount -= takeFromPriority
	}
	q.priorityMutex.Unlock()

	if maxCount > 0 {
		q.queueMutex.Lock()
		queueLen := len(q.queue)
		if queueLen > 0 {
			takeFromQueue := min(queueLen, maxCount)
			
			batch = append(batch, q.queue[:takeFromQueue]...)
			copy(q.queue, q.queue[takeFromQueue:])
			q.queue = q.queue[:queueLen-takeFromQueue]
		}
		q.queueMutex.Unlock()
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