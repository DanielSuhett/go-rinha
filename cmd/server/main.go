package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-rinha/internal/client"
	"go-rinha/internal/config"
	"go-rinha/internal/handler"
	"go-rinha/internal/repository"
	"go-rinha/internal/service"
	"go-rinha/pkg/health"
	"go-rinha/pkg/redis"

	"github.com/valyala/fasthttp"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	redisClient := redis.NewClient(cfg.GetRedisAddr())
	if err := redisClient.Ping(context.Background()); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	httpClient := client.NewHTTPClient()
	circuitBreaker := health.NewChecker(httpClient, redisClient, cfg)
	queueService := service.NewQueueService(cfg)
	paymentRepo := repository.NewPaymentRepository(httpClient, redisClient, cfg)
	paymentService := service.NewPaymentService(circuitBreaker, queueService, paymentRepo)

	queueService.SetPaymentProcessor(paymentService.ProcessPayment)

	paymentHandler := handler.NewPaymentHandler(paymentService, queueService)

	server := &fasthttp.Server{
		Handler:                       setupRoutes(paymentHandler),
		DisableKeepalive:              false,
		MaxRequestBodySize:            4 * 1024,
		ReadTimeout:                   100 * time.Millisecond,
		WriteTimeout:                  100 * time.Millisecond,
		IdleTimeout:                   10 * time.Second,
		MaxConnsPerIP:                 0,
		MaxRequestsPerConn:            0,
		TCPKeepalive:                  true,
		TCPKeepalivePeriod:            10 * time.Second,
		ReduceMemoryUsage:             false,
		GetOnly:                       false,
		DisablePreParseMultipartForm:  true,
		LogAllErrors:                  true,
		SecureErrorLogMessage:         false,
		DisableHeaderNamesNormalizing: true,
		NoDefaultServerHeader:         true,
		NoDefaultDate:                 true,
		NoDefaultContentType:          true,
		ReadBufferSize:                4096,
		WriteBufferSize:               4096,
		Concurrency:                   256 * 1024,
	}

	circuitBreaker.Start()
	queueService.Start()

	go func() {
		addr := fmt.Sprintf("0.0.0.0:%d", cfg.AppPort)
		if err := server.ListenAndServe(addr); err != nil {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	circuitBreaker.Stop()
	queueService.Stop()

	if err := server.ShutdownWithContext(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	redisClient.Close()
}

func setupRoutes(paymentHandler *handler.PaymentHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		path := string(ctx.Path())
		method := string(ctx.Method())

		switch {
		case method == "POST" && path == "/payments":
			paymentHandler.PostPayments(ctx)
		case method == "GET" && path == "/payments-summary":
			paymentHandler.GetPaymentsSummary(ctx)
		case method == "GET" && path == "/health":
			paymentHandler.GetHealth(ctx)
		default:
			ctx.SetStatusCode(fasthttp.StatusNotFound)
		}
	}
}
