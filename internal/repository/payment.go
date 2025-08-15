package repository

import (
	"bytes"
	"context"
	"fmt"
	"go-rinha/internal/client"
	"go-rinha/internal/config"
	"go-rinha/internal/types"
	"go-rinha/pkg/pool"
	"log"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
	"unsafe"

	redisClient "go-rinha/pkg/redis"

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

func UnsafeBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func UnsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (r *PaymentRepository) Send(processor types.Processor, data []byte, circuitBreaker interface {
	SignalFailure(types.Processor) types.CircuitBreakerColor
}, queueService interface{ Requeue([]byte) },
) error {
	jsonData := r.injectRequestedAt(data)

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

func (r *PaymentRepository) injectRequestedAt(data []byte) []byte {
	if bytes.Contains(data, []byte(`"requestedAt"`)) {
		return data
	}

	buffer := pool.GetByteBuffer()
	defer pool.PutByteBuffer(buffer)

	timestamp := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	requestedAtField := `,"requestedAt":"` + timestamp + `"`

	lastBraceIndex := bytes.LastIndexByte(data, '}')
	if lastBraceIndex == -1 {
		return data
	}

	buffer = append(buffer, data[:lastBraceIndex]...)
	buffer = append(buffer, requestedAtField...)
	buffer = append(buffer, data[lastBraceIndex:]...)

	result := make([]byte, len(buffer))
	copy(result, buffer)
	return result
}

func (r *PaymentRepository) extractJSONField(data []byte, field string) string {
	fieldPattern := `"` + field + `":`
	start := bytes.Index(data, UnsafeBytes(fieldPattern))
	if start == -1 {
		return ""
	}

	start += len(fieldPattern)
	for start < len(data) && (data[start] == ' ' || data[start] == '\t') {
		start++
	}

	if start >= len(data) || data[start] != '"' {
		return ""
	}
	start++

	end := start
	for end < len(data) && data[end] != '"' {
		if data[end] == '\\' && end+1 < len(data) {
			end += 2
		} else {
			end++
		}
	}

	if end >= len(data) {
		return ""
	}

	return UnsafeString(data[start:end])
}

func (r *PaymentRepository) extractAmount(data []byte) float64 {
	fieldPattern := `"amount":`
	start := bytes.Index(data, UnsafeBytes(fieldPattern))
	if start == -1 {
		return 0
	}

	start += len(fieldPattern)
	for start < len(data) && (data[start] == ' ' || data[start] == '\t') {
		start++
	}

	end := start
	for end < len(data) && (data[end] >= '0' && data[end] <= '9' || data[end] == '.') {
		end++
	}

	if end <= start {
		return 0
	}

	value, _ := strconv.ParseFloat(UnsafeString(data[start:end]), 64)
	return value
}

func (r *PaymentRepository) extractRequestedAt(data []byte) string {
	return r.extractJSONField(data, "requestedAt")
}

func (r *PaymentRepository) save(processor types.Processor, jsonData []byte) error {
	correlationID := r.extractJSONField(jsonData, "correlationId")
	amount := r.extractAmount(jsonData)
	requestedAt := r.extractRequestedAt(jsonData)

	timestamp, err := time.Parse(time.RFC3339, requestedAt)
	if err != nil {
		return fmt.Errorf("failed to parse timestamp: %w", err)
	}

	pipe := r.redisClient.Pipeline()

	buffer := pool.GetByteBuffer()
	defer pool.PutByteBuffer(buffer)

	buffer = strconv.AppendFloat(buffer, amount, 'f', 2, 64)
	buffer = append(buffer, ':')
	buffer = append(buffer, UnsafeBytes(correlationID)...)
	member := UnsafeString(buffer)

	timelineKeyBytes := pool.GetByteBuffer()
	defer pool.PutByteBuffer(timelineKeyBytes)
	timelineKeyBytes = append(timelineKeyBytes, processedPaymentsPrefix...)
	timelineKeyBytes = append(timelineKeyBytes, ':')
	timelineKeyBytes = append(timelineKeyBytes, UnsafeBytes(string(processor))...)
	timelineKeyBytes = append(timelineKeyBytes, ":timeline"...)
	timelineKey := UnsafeString(timelineKeyBytes)

	statsKeyBytes := pool.GetByteBuffer()
	defer pool.PutByteBuffer(statsKeyBytes)
	statsKeyBytes = append(statsKeyBytes, processedPaymentsPrefix...)
	statsKeyBytes = append(statsKeyBytes, ':')
	statsKeyBytes = append(statsKeyBytes, processor...)
	statsKeyBytes = append(statsKeyBytes, ":stats"...)
	statsKey := UnsafeString(statsKeyBytes)

	pipe.ZAddNX(r.ctx, timelineKey, redis.Z{
		Score:  float64(timestamp.UnixMilli()),
		Member: member,
	})
	pipe.HIncrBy(r.ctx, statsKey, "count", 1)
	pipe.HIncrByFloat(r.ctx, statsKey, "total", amount)

	_, err = pipe.Exec(r.ctx)
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
