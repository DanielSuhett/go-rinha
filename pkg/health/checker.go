package health

import (
	"context"
	"encoding/json"
	"go-rinha/internal/client"
	"go-rinha/internal/config"
	"go-rinha/internal/types"
	redisClient "go-rinha/pkg/redis"
	"log"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const colorDebounceMsec = 3000

type Checker struct {
	httpClient      *client.HTTPClient
	redisClient     *redisClient.Client
	config          *config.Config
	currentColor    atomic.Value
	ctx             context.Context
	cancel          context.CancelFunc
	lastColorChange atomic.Int64
	recoveryTicker  *time.Ticker
	pubSub          *redis.PubSub
	lastPublished   atomic.Value
}

func NewChecker(httpClient *client.HTTPClient, redisClient *redisClient.Client, config *config.Config) *Checker {
	ctx, cancel := context.WithCancel(context.Background())

	checker := &Checker{
		httpClient:  httpClient,
		redisClient: redisClient,
		config:      config,
		ctx:         ctx,
		cancel:      cancel,
	}

	checker.currentColor.Store(types.ColorGreen)
	checker.lastColorChange.Store(0)
	checker.lastPublished.Store(types.CircuitBreakerColor(""))

	return checker
}

func (c *Checker) Start() {
	go c.subscribeToColorChanges()
	if c.config.IsMaster() {
		go c.healthCheckWorker()
	}
}

func (c *Checker) Stop() {
	if c.recoveryTicker != nil {
		c.recoveryTicker.Stop()
	}
	if c.pubSub != nil {
		c.pubSub.Close()
	}
	c.cancel()
}

func (c *Checker) GetCurrentColor() types.CircuitBreakerColor {
	return c.currentColor.Load().(types.CircuitBreakerColor)
}

func (c *Checker) SignalFailure(processor types.Processor) types.CircuitBreakerColor {
	log.Printf("Signal failure for %s", processor)
	otherProcessor := types.ProcessorFallback
	if processor == types.ProcessorFallback {
		otherProcessor = types.ProcessorDefault
	}

	health := c.checkHealth(otherProcessor)
	if health != nil && !health.Failing {
		var newColor types.CircuitBreakerColor
		if otherProcessor == types.ProcessorFallback {
			newColor = types.ColorYellow
		} else {
			newColor = types.ColorGreen
		}
		c.setColor(newColor)
	} else {
		c.setColor(types.ColorRed)
	}

	return c.GetCurrentColor()
}

func (c *Checker) setColor(color types.CircuitBreakerColor) {
	now := time.Now().UnixMilli()

	log.Printf("Setting color to %s", color)
	currentColor := c.currentColor.Load().(types.CircuitBreakerColor)
	if currentColor != color {
		lastChange := c.lastColorChange.Load()
		if color == types.ColorRed || now-lastChange >= colorDebounceMsec {
			c.currentColor.Store(color)
			c.lastColorChange.Store(now)
			if c.config.IsMaster() {
				c.publishColorChange(color)
			}
		}
	}
}

func (c *Checker) healthCheckWorker() {
	ticker := time.NewTicker(time.Duration(c.config.HealthInterval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.performHealthChecks()
		}
	}
}

func (c *Checker) startRecoveryMonitoring() {
	if c.recoveryTicker != nil {
		return
	}

	c.recoveryTicker = time.NewTicker(time.Duration(c.config.HealthInterval) * time.Millisecond)

	go func() {
		defer func() {
			if c.recoveryTicker != nil {
				c.recoveryTicker.Stop()
				c.recoveryTicker = nil
			}
		}()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-c.recoveryTicker.C:
				currentColor := c.GetCurrentColor()
				if currentColor != types.ColorRed {
					return
				}

				recoveredColor := c.checkForRecovery()
				if recoveredColor != types.ColorRed {
					c.setColor(recoveredColor)
					return
				}
			}
		}
	}()
}

func (c *Checker) performHealthChecks() {
	recoveredColor := c.checkForRecovery()
	c.setColor(recoveredColor)
}

func (c *Checker) checkForRecovery() types.CircuitBreakerColor {
	defaultHealth := c.checkHealth(types.ProcessorDefault)
	fallbackHealth := c.checkHealth(types.ProcessorFallback)

	processors := map[string]*types.ProcessorHealth{
		"default":  defaultHealth,
		"fallback": fallbackHealth,
	}

	if defaultHealth == nil {
		processors["default"] = &types.ProcessorHealth{Failing: true, MinResponseTime: 0}
	}
	if fallbackHealth == nil {
		processors["fallback"] = &types.ProcessorHealth{Failing: true, MinResponseTime: 0}
	}

	return c.calculateColor(processors)
}

func (c *Checker) checkHealth(processor types.Processor) *types.ProcessorHealth {
	url := c.config.GetProcessorHealthURL(string(processor))
	if url == "" {
		return nil
	}

	timeout := time.Duration(c.config.HealthTimeout) * time.Millisecond
	statusCode, body, duration, err := c.httpClient.GetHealth(url, timeout)
	if err != nil {
		return &types.ProcessorHealth{MinResponseTime: 0, Failing: true}
	}

	if statusCode == 429 {
		return &types.ProcessorHealth{MinResponseTime: 0, Failing: false}
	}

	if statusCode != 200 && statusCode != 201 {
		return &types.ProcessorHealth{MinResponseTime: 0, Failing: true}
	}

	var health types.ProcessorHealth
	if err := json.Unmarshal(body, &health); err != nil {
		return &types.ProcessorHealth{
			MinResponseTime: int(duration.Milliseconds()),
			Failing:         false,
		}
	}

	return &health
}

func (c *Checker) calculateColor(processors map[string]*types.ProcessorHealth) types.CircuitBreakerColor {
	defaultHealth := processors["default"]
	fallbackHealth := processors["fallback"]

	diff := defaultHealth.MinResponseTime - fallbackHealth.MinResponseTime

	if defaultHealth.Failing && fallbackHealth.Failing {
		return types.ColorRed
	}

	if defaultHealth.Failing && !fallbackHealth.Failing {
		return types.ColorYellow
	}

	if !defaultHealth.Failing && !fallbackHealth.Failing && diff >= c.config.LatencyDiffToUseFallback {
		return types.ColorYellow
	}

	return types.ColorGreen
}

func (c *Checker) subscribeToColorChanges() {
	c.pubSub = c.redisClient.Subscribe(c.ctx, types.ChannelCircuitColor)
	defer c.pubSub.Close()

	ch := c.pubSub.Channel()
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-ch:
			if msg == nil {
				continue
			}

			color := types.CircuitBreakerColor(msg.Payload)
			log.Printf("Received color change: %s", color)
			c.currentColor.Store(color)
			c.lastColorChange.Store(time.Now().UnixMilli())
		}
	}
}

func (c *Checker) publishColorChange(color types.CircuitBreakerColor) {
	lastPublished := c.lastPublished.Load().(types.CircuitBreakerColor)
	if lastPublished == color {
		return
	}
	
	if err := c.redisClient.Publish(c.ctx, types.ChannelCircuitColor, string(color)); err != nil {
		log.Printf("Failed to publish color change: %v", err)
		return
	}
	
	c.lastPublished.Store(color)
	log.Printf("Published color change: %s", color)
}
