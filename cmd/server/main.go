package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/valyala/fasthttp"
	"go-rinha/internal/client"
	"go-rinha/internal/config"
	"go-rinha/internal/handler"
	"go-rinha/internal/repository"
	"go-rinha/internal/service"
	"go-rinha/pkg/health"
	"go-rinha/pkg/redis"
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
	queueService := service.NewQueueService(redisClient, cfg)
	paymentRepo := repository.NewPaymentRepository(httpClient, redisClient, cfg)
	paymentService := service.NewPaymentService(circuitBreaker, queueService, paymentRepo)
	
	queueService.SetPaymentProcessor(paymentService.ProcessPayment)

	paymentHandler := handler.NewPaymentHandler(paymentService, queueService)

	server := &fasthttp.Server{
		Handler:               setupRoutes(paymentHandler),
		DisableKeepalive:      false,
		MaxRequestBodySize:    16 * 1024,
		ReadTimeout:           5 * time.Second,
		WriteTimeout:          5 * time.Second,
		IdleTimeout:           72 * time.Second,
		MaxConnsPerIP:         0,
		MaxRequestsPerConn:    0,
		TCPKeepalive:          true,
		TCPKeepalivePeriod:    30 * time.Second,
		ReduceMemoryUsage:     true,
		GetOnly:               false,
		DisablePreParseMultipartForm: true,
		LogAllErrors:          false,
		SecureErrorLogMessage: false,
		DisableHeaderNamesNormalizing: true,
		NoDefaultServerHeader: true,
		NoDefaultDate:         true,
		NoDefaultContentType:  true,
	}

	circuitBreaker.Start()
	queueService.Start()

	go func() {
		addr := fmt.Sprintf("0.0.0.0:%d", cfg.AppPort)
		log.Printf("Server starting on %s", addr)
		if err := server.ListenAndServe(addr); err != nil {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	circuitBreaker.Stop()
	queueService.Stop()

	if err := server.ShutdownWithContext(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	redisClient.Close()
	log.Println("Server exited")
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