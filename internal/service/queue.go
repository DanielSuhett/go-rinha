package service

import (
	"context"
	"go-rinha/internal/config"
	"go-rinha/pkg/redis"
	"log"
	"math"
	"sync"
	"time"
)

type QueueService struct {
	redisClient      *redis.Client
	config           *config.Config
	ctx              context.Context
	cancel           context.CancelFunc
	paymentProcessor func(string) error
	isProcessing     bool
	backoffDelay     time.Duration

	workerChan  chan string
	workerWg    sync.WaitGroup
	workerCount int

	requeueChan  chan string
	requeueWg    sync.WaitGroup
	requeueCount int
}

const (
	minBackoff        = 500 * time.Millisecond
	maxBackoff        = 1000 * time.Millisecond
	backoffMultiplier = 1.2
	workerPoolSize    = 8
	workerChanBuffer  = 400
	requeueWorkerSize = 24
	requeueChanBuffer = 3000
)

func NewQueueService(redisClient *redis.Client, config *config.Config) *QueueService {
	ctx, cancel := context.WithCancel(context.Background())

	q := &QueueService{
		redisClient:  redisClient,
		config:       config,
		ctx:          ctx,
		cancel:       cancel,
		backoffDelay: minBackoff,
		workerChan:   make(chan string, workerChanBuffer),
		workerCount:  workerPoolSize,
		requeueChan:  make(chan string, requeueChanBuffer),
		requeueCount: requeueWorkerSize,
	}

	q.startWorkers()
	q.startRequeueWorkers()
	return q
}

func (q *QueueService) SetPaymentProcessor(processor func(string) error) {
	q.paymentProcessor = processor
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
			if err := q.redisClient.LPush(q.ctx, q.config.GetRedisKeyPrefix(), item); err != nil {
				log.Printf("Failed to push item to Redis: %v", err)
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
			if err := q.redisClient.LPush(q.ctx, q.config.GetRedisKeyPrefix(), item); err != nil {
				log.Printf("Failed to requeue item to Redis: %v", err)
			}
		}
	}
}

func (q *QueueService) Add(item string) {
	select {
	case q.workerChan <- item:
	case <-q.ctx.Done():
		log.Printf("Queue service stopped, cannot add item")
	default:
		log.Printf("Worker channel full, attempting direct Redis push")
		if err := q.redisClient.LPush(q.ctx, q.config.GetRedisKeyPrefix(), item); err != nil {
			log.Printf("Failed to push item to Redis: %v", err)
		}
	}
}

func (q *QueueService) Requeue(item string) {
	select {
	case q.requeueChan <- item:
	case <-q.ctx.Done():
		log.Printf("Queue service stopped, cannot requeue item")
	default:
		log.Printf("Requeue channel full, attempting direct Redis push")
		if err := q.redisClient.LPush(q.ctx, q.config.GetRedisKeyPrefix(), item); err != nil {
			log.Printf("Failed to requeue item to Redis: %v", err)
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
	batch, err := q.redisClient.Dequeue(q.ctx, q.config.GetRedisKeyPrefix(), 25)
	if err != nil {
		time.Sleep(time.Duration(q.config.PollingInterval) * time.Millisecond)
		log.Printf("Failed to process batch: %v", err)
		return
	}

	if len(batch) > 0 {
		q.backoffDelay = minBackoff
		q.processItems(batch)
	} else {
		q.exponentialBackoff()
	}
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

func (q *QueueService) exponentialBackoff() {
	time.Sleep(q.backoffDelay)
	q.backoffDelay = time.Duration(math.Min(
		float64(q.backoffDelay)*backoffMultiplier,
		float64(maxBackoff),
	))
}
