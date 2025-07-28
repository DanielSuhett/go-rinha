package service

import (
	"context"
	"go-rinha/internal/config"
	"go-rinha/pkg/redis"
	"log"
	"math"
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
}

const (
	minBackoff        = 100 * time.Millisecond
	maxBackoff        = 1000 * time.Millisecond
	backoffMultiplier = 1.1
)

func NewQueueService(redisClient *redis.Client, config *config.Config) *QueueService {
	ctx, cancel := context.WithCancel(context.Background())

	return &QueueService{
		redisClient:  redisClient,
		config:       config,
		ctx:          ctx,
		cancel:       cancel,
		backoffDelay: minBackoff,
	}
}

func (q *QueueService) SetPaymentProcessor(processor func(string) error) {
	q.paymentProcessor = processor
}

func (q *QueueService) Add(item string) {
	go func() {
		if err := q.redisClient.LPush(q.ctx, q.config.GetRedisKeyPrefix(), item); err != nil {
			log.Printf("Failed to add item to queue: %v", err)
		}
	}()
}

func (q *QueueService) Requeue(item string) {
	go func() {
		if err := q.redisClient.LPush(q.ctx, q.config.GetRedisKeyPrefix(), item); err != nil {
			log.Printf("Failed to requeue item: %v", err)
		}
	}()
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
	batch, err := q.redisClient.Dequeue(q.ctx, q.config.GetRedisKeyPrefix(), 10)
	if err != nil {
		time.Sleep(time.Duration(q.config.PollingInterval) * time.Millisecond)
		log.Printf("Failed to process batch: %v", err)
		return
	}

	if len(batch) > 0 {
		q.backoffDelay = minBackoff
		q.processItems(batch)
	} else {
		log.Printf("No items in batch")
		q.exponentialBackoff()
	}
}

func (q *QueueService) processItems(items []string) {
	for _, item := range items {
		if q.paymentProcessor != nil {
			if err := q.paymentProcessor(item); err != nil {
				log.Printf("Payment processing error: %v", err)
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

