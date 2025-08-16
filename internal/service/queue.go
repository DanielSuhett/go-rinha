package service

import (
	"context"
	"go-rinha/internal/config"
	"go-rinha/internal/types"
	"go-rinha/pkg/utils"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var batchPool = sync.Pool{
	New: func() any {
		batch := make([][]byte, 0, 64)
		return &batch
	},
}

type QueueService struct {
	buffer           [][]byte
	head             uint64
	tail             uint64
	mask             uint64
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
	if queueSize&(queueSize-1) != 0 {
		queueSize = utils.NextPowerOf2(queueSize)
	}

	q := &QueueService{
		buffer: make([][]byte, queueSize),
		mask:   uint64(queueSize - 1),
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}

	return q
}

func (q *QueueService) SetPaymentProcessor(processor func([]byte) error) {
	q.paymentProcessor = processor
}

func (q *QueueService) SetHealthChecker(healthChecker HealthChecker) {
	q.healthChecker = healthChecker
}

func (q *QueueService) Push(data []byte) bool {
	for {
		head := atomic.LoadUint64(&q.head)
		next := head + 1

		if next&q.mask == atomic.LoadUint64(&q.tail)&q.mask {
			return false
		}

		if atomic.CompareAndSwapUint64(&q.head, head, next) {
			q.buffer[head&q.mask] = data
			return true
		}
	}
}

func (q *QueueService) Pop() ([]byte, bool) {
	tail := atomic.LoadUint64(&q.tail)
	head := atomic.LoadUint64(&q.head)

	if tail == head {
		return nil, false
	}

	data := q.buffer[tail&q.mask]
	atomic.StoreUint64(&q.tail, tail+1)
	return data, true
}

func (q *QueueService) PopBatch(maxCount int) [][]byte {
	if maxCount <= 0 {
		return nil
	}

	tail := atomic.LoadUint64(&q.tail)
	head := atomic.LoadUint64(&q.head)

	available := int(head - tail)
	if available <= 0 {
		return nil
	}

	if available > maxCount {
		available = maxCount
	}

	batch := (*batchPool.Get().(*[][]byte))[:0]
	if cap(batch) < available {
		batchPool.Put(&batch)
		batch = make([][]byte, 0, available)
	}

	for i := 0; i < available; i++ {
		batch = append(batch, q.buffer[(tail+uint64(i))&q.mask])
	}

	atomic.StoreUint64(&q.tail, tail+uint64(available))
	return batch
}

func (q *QueueService) ReleaseBatch(batch [][]byte) {
	if batch != nil && cap(batch) <= 256 {
		batchPool.Put(&batch)
	}
}

func (q *QueueService) Size() int {
	head := atomic.LoadUint64(&q.head)
	tail := atomic.LoadUint64(&q.tail)
	return int(head - tail)
}

func (q *QueueService) Add(item []byte) {
	if !q.Push(item) {
		log.Printf("Failed to push item to queue: queue full")
	}
}

func (q *QueueService) Requeue(item []byte) {
	if !q.Push(item) {
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

	batchBytes := q.PopBatch(q.config.BatchSize)
	if len(batchBytes) == 0 {
		time.Sleep(time.Duration(q.config.PollingInterval) * time.Millisecond)
		return
	}
	defer q.ReleaseBatch(batchBytes)

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
