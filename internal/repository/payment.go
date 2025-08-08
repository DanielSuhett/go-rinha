package repository

import (
	"context"
	"fmt"
	"go-rinha/internal/client"
	"go-rinha/internal/config"
	"go-rinha/internal/types"
	"log"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	redisClient "go-rinha/pkg/redis"

	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"
)

type PaymentRepository struct {
	httpClient  *client.HTTPClient
	redisClient *redisClient.Client
	config      *config.Config
	ctx         context.Context
}

const processedPaymentsPrefix = "processed:payments"

func NewPaymentRepository(httpClient *client.HTTPClient, redisClient *redisClient.Client, config *config.Config) *PaymentRepository {
	return &PaymentRepository{
		httpClient:  httpClient,
		redisClient: redisClient,
		config:      config,
		ctx:         context.Background(),
	}
}

func (r *PaymentRepository) Send(processor types.Processor, data string, circuitBreaker interface {
	SignalFailure(types.Processor) types.CircuitBreakerColor
}, queueService interface{ Requeue(string) },
) error {
	payment, err := r.parsePaymentWithTimestamp(data)
	if err != nil {
		return fmt.Errorf("failed to parse payment: %w", err)
	}

	url := r.config.GetProcessorPaymentURL(string(processor))
	if url == "" {
		return fmt.Errorf("invalid processor: %s", processor)
	}

	statusCode, err := r.httpClient.PostPayment(url, payment)

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			log.Printf("Timeout error with processor %s", processor)
			queueService.Requeue(data)
			return nil
		}
		circuitBreaker.SignalFailure(processor)
		queueService.Requeue(data)
		return nil
	}

	switch statusCode {
	case 200, 201:
		go func(processor types.Processor, payment *types.PaymentRequest) {
			if err := r.save(processor, payment.Amount, payment.CorrelationID, payment.RequestedAt); err != nil {
				log.Printf("Failed to save payment: %v", err)
			}
		}(processor, payment)

		return nil
	case 422:
		log.Printf("Payment already exists (422): %s - counting as successful", payment.CorrelationID)
		if err := r.save(processor, payment.Amount, payment.CorrelationID, payment.RequestedAt); err != nil {
			log.Printf("Failed to save duplicate payment: %v", err)
			return err
		}
		return nil
	default:
		circuitBreaker.SignalFailure(processor)
		queueService.Requeue(data)
		return nil
	}
}

type CircuitBreaker interface {
	SignalFailure(processor types.Processor) types.CircuitBreakerColor
}

type QueueService interface {
	Requeue(item string)
}

func (r *PaymentRepository) SendBatch(processor types.Processor, items []string, circuitBreaker CircuitBreaker, queueService QueueService) error {
	var successfulPayments []BatchPayment
	var failedItems []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	url := r.config.GetProcessorPaymentURL(string(processor))
	if url == "" {
		for _, item := range items {
			queueService.Requeue(item)
		}
		return nil
	}

	for _, data := range items {
		payment, err := r.parsePaymentWithTimestamp(data)
		if err != nil {
			mu.Lock()
			failedItems = append(failedItems, data)
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func(data string, payment *types.PaymentRequest) {
			defer wg.Done()
			
			statusCode, err := r.httpClient.PostPayment(url, payment)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					log.Printf("Timeout error with processor %s", processor)
				} else {
					circuitBreaker.SignalFailure(processor)
				}
				failedItems = append(failedItems, data)
				return
			}

			switch statusCode {
			case 200, 201, 422:
				successfulPayments = append(successfulPayments, BatchPayment{
					Processor:     processor,
					Amount:        payment.Amount,
					CorrelationID: payment.CorrelationID,
					RequestedAt:   payment.RequestedAt,
				})
			default:
				circuitBreaker.SignalFailure(processor)
				failedItems = append(failedItems, data)
			}
		}(data, payment)
	}

	wg.Wait()

	if len(successfulPayments) > 0 {
		if err := r.saveBatch(successfulPayments); err != nil {
			log.Printf("Failed to save batch payments: %v", err)
		}
	}

	for _, item := range failedItems {
		queueService.Requeue(item)
	}

	return nil
}

func (r *PaymentRepository) parsePaymentWithTimestamp(data string) (*types.PaymentRequest, error) {
	var payment types.PaymentRequest

	if err := sonic.ConfigFastest.UnmarshalFromString(data, &payment); err != nil {
		return nil, err
	}

	if payment.RequestedAt == "" {
		payment.RequestedAt = time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	}

	return &payment, nil
}

func (r *PaymentRepository) save(processor types.Processor, amount float64, correlationID, requestedAt string) error {
	start := time.Now()
	
	timestamp, err := time.Parse(time.RFC3339, requestedAt)
	if err != nil {
		return fmt.Errorf("failed to parse timestamp: %w", err)
	}

	pipe := r.redisClient.Pipeline()

	timelineKey := fmt.Sprintf("%s:%s:timeline", processedPaymentsPrefix, processor)
	statsKey := fmt.Sprintf("%s:%s:stats", processedPaymentsPrefix, processor)

	pipe.ZAddNX(r.ctx, timelineKey, redis.Z{
		Score:  float64(timestamp.UnixMilli()),
		Member: fmt.Sprintf("%.2f:%s", amount, correlationID),
	})
	pipe.HIncrBy(r.ctx, statsKey, "count", 1)
	pipe.HIncrByFloat(r.ctx, statsKey, "total", amount)

	execStart := time.Now()
	_, err = pipe.Exec(r.ctx)
	execDuration := time.Since(execStart)
	
	totalDuration := time.Since(start)
	if totalDuration >= time.Millisecond {
		log.Printf("PERF: Redis save total=%v exec=%v", totalDuration, execDuration)
	}
	
	return err
}

type BatchPayment struct {
	Processor   types.Processor
	Amount      float64
	CorrelationID string
	RequestedAt string
}

func (r *PaymentRepository) saveBatch(payments []BatchPayment) error {
	if len(payments) == 0 {
		return nil
	}

	start := time.Now()
	pipe := r.redisClient.Pipeline()

	for _, payment := range payments {
		timestamp, err := time.Parse(time.RFC3339, payment.RequestedAt)
		if err != nil {
			continue
		}

		timelineKey := fmt.Sprintf("%s:%s:timeline", processedPaymentsPrefix, payment.Processor)
		statsKey := fmt.Sprintf("%s:%s:stats", processedPaymentsPrefix, payment.Processor)

		pipe.ZAddNX(r.ctx, timelineKey, redis.Z{
			Score:  float64(timestamp.UnixMilli()),
			Member: fmt.Sprintf("%.2f:%s", payment.Amount, payment.CorrelationID),
		})
		pipe.HIncrBy(r.ctx, statsKey, "count", 1)
		pipe.HIncrByFloat(r.ctx, statsKey, "total", payment.Amount)
	}

	execStart := time.Now()
	_, err := pipe.Exec(r.ctx)
	execDuration := time.Since(execStart)
	
	totalDuration := time.Since(start)
	if totalDuration >= time.Millisecond {
		log.Printf("PERF: Redis saveBatch(%d payments) total=%v exec=%v", len(payments), totalDuration, execDuration)
	}
	
	return err
}

func (r *PaymentRepository) Find(processor types.Processor, fromTime, toTime *int64) (*types.PaymentSummary, error) {
	statsKey := fmt.Sprintf("%s:%s:stats", processedPaymentsPrefix, processor)

	if fromTime == nil && toTime == nil {
		results, err := r.redisClient.HMGet(r.ctx, statsKey, "count", "total")
		if err != nil {
			return &types.PaymentSummary{}, nil
		}

		count := 0
		total := 0.0

		if results[0] != nil {
			if countStr, ok := results[0].(string); ok {
				count, _ = strconv.Atoi(countStr)
			}
		}

		if results[1] != nil {
			if totalStr, ok := results[1].(string); ok {
				total, _ = strconv.ParseFloat(totalStr, 64)
			}
		}

		return &types.PaymentSummary{
			TotalRequests: count,
			TotalAmount:   math.Round(total*100) / 100,
		}, nil
	}

	timelineKey := fmt.Sprintf("%s:%s:timeline", processedPaymentsPrefix, processor)
	min := "0"
	max := "+inf"

	if fromTime != nil {
		min = strconv.FormatInt(*fromTime, 10)
	}
	if toTime != nil {
		max = strconv.FormatInt(*toTime, 10)
	}

	amounts, err := r.redisClient.ZRangeByScore(r.ctx, timelineKey, &redis.ZRangeBy{
		Min: min,
		Max: max,
	})
	if err != nil {
		return &types.PaymentSummary{}, nil
	}

	if len(amounts) == 0 {
		return &types.PaymentSummary{TotalRequests: 0, TotalAmount: 0}, nil
	}

	totalRequests := len(amounts)
	totalAmount := 0.0

	for _, memberStr := range amounts {
		parts := strings.Split(memberStr, ":")
		if len(parts) > 0 {
			if amount, err := strconv.ParseFloat(parts[0], 64); err == nil {
				totalAmount += amount
			}
		}
	}

	return &types.PaymentSummary{
		TotalRequests: totalRequests,
		TotalAmount:   math.Round(totalAmount*100) / 100,
	}, nil
}

func (r *PaymentRepository) FindAll(fromTime, toTime *int64) (*types.PaymentSummaryResponse, error) {
	if fromTime == nil && toTime == nil {
		defaultStatsKey := fmt.Sprintf("%s:%s:stats", processedPaymentsPrefix, types.ProcessorDefault)
		fallbackStatsKey := fmt.Sprintf("%s:%s:stats", processedPaymentsPrefix, types.ProcessorFallback)

		pipe := r.redisClient.Pipeline()
		pipe.HMGet(r.ctx, defaultStatsKey, "count", "total")
		pipe.HMGet(r.ctx, fallbackStatsKey, "count", "total")

		results, err := pipe.Exec(r.ctx)
		if err != nil || len(results) < 2 {
			return &types.PaymentSummaryResponse{
				Default:  types.PaymentSummary{TotalRequests: 0, TotalAmount: 0},
				Fallback: types.PaymentSummary{TotalRequests: 0, TotalAmount: 0},
			}, nil
		}

		defaultResults := results[0].(*redis.SliceCmd).Val()
		fallbackResults := results[1].(*redis.SliceCmd).Val()

		return &types.PaymentSummaryResponse{
			Default:  r.parseStatsResults(defaultResults),
			Fallback: r.parseStatsResults(fallbackResults),
		}, nil
	}

	defaultTimelineKey := fmt.Sprintf("%s:%s:timeline", processedPaymentsPrefix, types.ProcessorDefault)
	fallbackTimelineKey := fmt.Sprintf("%s:%s:timeline", processedPaymentsPrefix, types.ProcessorFallback)

	min := "0"
	max := "+inf"

	if fromTime != nil {
		min = strconv.FormatInt(*fromTime, 10)
	}
	if toTime != nil {
		max = strconv.FormatInt(*toTime, 10)
	}

	pipe := r.redisClient.Pipeline()
	pipe.ZRangeByScore(r.ctx, defaultTimelineKey, &redis.ZRangeBy{Min: min, Max: max})
	pipe.ZRangeByScore(r.ctx, fallbackTimelineKey, &redis.ZRangeBy{Min: min, Max: max})

	results, err := pipe.Exec(r.ctx)
	if err != nil || len(results) < 2 {
		return &types.PaymentSummaryResponse{
			Default:  types.PaymentSummary{TotalRequests: 0, TotalAmount: 0},
			Fallback: types.PaymentSummary{TotalRequests: 0, TotalAmount: 0},
		}, nil
	}

	defaultAmounts := results[0].(*redis.StringSliceCmd).Val()
	fallbackAmounts := results[1].(*redis.StringSliceCmd).Val()

	return &types.PaymentSummaryResponse{
		Default:  r.processAmounts(defaultAmounts),
		Fallback: r.processAmounts(fallbackAmounts),
	}, nil
}

func (r *PaymentRepository) parseStatsResults(results []interface{}) types.PaymentSummary {
	count := 0
	total := 0.0

	if len(results) >= 2 {
		if results[0] != nil {
			if countStr, ok := results[0].(string); ok {
				count, _ = strconv.Atoi(countStr)
			}
		}
		if results[1] != nil {
			if totalStr, ok := results[1].(string); ok {
				total, _ = strconv.ParseFloat(totalStr, 64)
			}
		}
	}

	return types.PaymentSummary{
		TotalRequests: count,
		TotalAmount:   math.Round(total*100) / 100,
	}
}

func (r *PaymentRepository) processAmounts(amounts []string) types.PaymentSummary {
	if len(amounts) == 0 {
		return types.PaymentSummary{TotalRequests: 0, TotalAmount: 0}
	}

	totalRequests := len(amounts)
	totalAmount := 0.0

	for _, memberStr := range amounts {
		parts := strings.Split(memberStr, ":")
		if len(parts) > 0 {
			if amount, err := strconv.ParseFloat(parts[0], 64); err == nil {
				totalAmount += amount
			}
		}
	}

	return types.PaymentSummary{
		TotalRequests: totalRequests,
		TotalAmount:   math.Round(totalAmount*100) / 100,
	}
}
