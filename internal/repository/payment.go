package repository

import (
	"context"
	"fmt"
	"go-rinha/internal/client"
	"go-rinha/internal/config"
	"go-rinha/internal/types"
	"go-rinha/pkg/utils"
	"log"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	redisClient "go-rinha/pkg/redis"

	"github.com/redis/go-redis/v9"
)

type PaymentRepository struct {
	httpClient  *client.HTTPClient
	redisClient *redisClient.Client
	config      *config.Config
	ctx         context.Context
}

func NewPaymentRepository(httpClient *client.HTTPClient, redisClient *redisClient.Client, config *config.Config) *PaymentRepository {
	return &PaymentRepository{
		httpClient:  httpClient,
		redisClient: redisClient,
		config:      config,
		ctx:         context.Background(),
	}
}

func (r *PaymentRepository) Send(processor types.Processor, data []byte, circuitBreaker interface {
	SignalFailure(types.Processor) types.CircuitBreakerColor
}, queueService interface{ Requeue([]byte) },
) error {
	jsonData := utils.InjectRequestedAt(data)

	url := r.config.GetProcessorPaymentURL(string(processor))
	if url == "" {
		return fmt.Errorf("invalid processor: %s", processor)
	}

	statusCode, err := r.httpClient.PostPayment(url, jsonData)

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			log.Printf("Timeout error with processor %s: %v", processor, err)
			queueService.Requeue(data)
			return nil
		}
		log.Printf("Request error with processor %s", processor)
		circuitBreaker.SignalFailure(processor)
		queueService.Requeue(data)
		return nil
	}

	switch statusCode {
	case 200, 201, 422:
		go r.save(processor, jsonData)
		return nil
	default:
		log.Printf("Non-success status %d from processor %s", statusCode, processor)
		circuitBreaker.SignalFailure(processor)
		queueService.Requeue(data)
		return nil
	}
}

func (r *PaymentRepository) save(processor types.Processor, jsonData []byte) error {
	correlationID := utils.ExtractJSONField(jsonData, "correlationId")
	amount := utils.ExtractAmount(jsonData)
	requestedAt := utils.ExtractRequestedAt(jsonData)

	timestamp, err := time.Parse(time.RFC3339, requestedAt)
	if err != nil {
		return fmt.Errorf("failed to parse timestamp: %w", err)
	}

	pipe := r.redisClient.Pipeline()

	member := utils.BuildSortedSetMember(amount, correlationID)
	timelineKey := utils.BuildTimelineKey(string(processor))
	statsKey := utils.BuildStatsKey(string(processor))

	pipe.ZAddNX(r.ctx, timelineKey, redis.Z{
		Score:  float64(timestamp.UnixMilli()),
		Member: member,
	})
	pipe.HIncrBy(r.ctx, statsKey, "count", 1)
	pipe.HIncrByFloat(r.ctx, statsKey, "total", amount)

	_, err = pipe.Exec(r.ctx)
	return err
}

func (r *PaymentRepository) FindAll(fromTime, toTime *int64) (*types.PaymentSummaryResponse, error) {
	if fromTime == nil && toTime == nil {
		defaultStatsKey := utils.BuildStatsKey(string(types.ProcessorDefault))
		fallbackStatsKey := utils.BuildStatsKey(string(types.ProcessorFallback))

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

	defaultTimelineKey := utils.BuildTimelineKey(string(types.ProcessorDefault))
	fallbackTimelineKey := utils.BuildTimelineKey(string(types.ProcessorFallback))

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
