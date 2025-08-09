package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	AppPort                  int    `validate:"required"`
	AppName                  string `validate:"required,oneof=1 2"`
	RedisHost                string `validate:"required"`
	RedisPort                int    `validate:"required"`
	ProcessorDefaultURL      string `validate:"required,url"`
	ProcessorFallbackURL     string `validate:"required,url"`
	PollingInterval          int    `validate:"required"`
	HealthTimeout            int    `validate:"required"`
	HealthInterval           int    `validate:"required"`
	LatencyDiffToUseFallback int    `validate:"required"`
	BatchSize                int    `validate:"required"`
	WorkerCount              int    `validate:"required"`
}

func LoadConfig() (*Config, error) {
	config := &Config{
		AppPort:                  getEnvAsInt("APP_PORT", 3000),
		AppName:                  getEnv("APP_NAME", "1"),
		RedisHost:                getEnv("REDIS_HOST", "localhost"),
		RedisPort:                getEnvAsInt("REDIS_PORT", 6380),
		ProcessorDefaultURL:      getEnv("PROCESSOR_DEFAULT_URL", ""),
		ProcessorFallbackURL:     getEnv("PROCESSOR_FALLBACK_URL", ""),
		PollingInterval:          getEnvAsInt("POOLING_INTERVAL", 2000),
		HealthTimeout:            getEnvAsInt("HEALTH_TIMEOUT", 500),
		HealthInterval:           getEnvAsInt("HEALTH_INTERVAL", 3000),
		LatencyDiffToUseFallback: getEnvAsInt("LATENCY_DIFF_TO_USE_FALLBACK", 5000),
		BatchSize:                getEnvAsInt("BATCH_SIZE", 50),
		WorkerCount:              getEnvAsInt("WORKER_COUNT", 2),
	}

	if err := ValidateConfig(config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return config, nil
}

func (c *Config) GetRedisAddr() string {
	return fmt.Sprintf("%s:%d", c.RedisHost, c.RedisPort)
}

func (c *Config) GetRedisKeyPrefix() string {
	return fmt.Sprintf("rinha:queue:%s", c.AppName)
}

func (c *Config) GetProcessorPaymentURL(processor string) string {
	switch processor {
	case "default":
		return c.ProcessorDefaultURL + "/payments"
	case "fallback":
		return c.ProcessorFallbackURL + "/payments"
	default:
		return ""
	}
}

func (c *Config) GetProcessorHealthURL(processor string) string {
	switch processor {
	case "default":
		return c.ProcessorDefaultURL + "/payments/service-health"
	case "fallback":
		return c.ProcessorFallbackURL + "/payments/service-health"
	default:
		return ""
	}
}

func (c *Config) IsMaster() bool {
	return c.AppName == "1"
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

