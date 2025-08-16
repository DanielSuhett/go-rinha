package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-rinha/internal/client"
	"go-rinha/internal/config"
	"go-rinha/internal/repository"
	"go-rinha/internal/service"
	"go-rinha/pkg/health"
	"go-rinha/pkg/redis"
	"go-rinha/pkg/utils"

	"github.com/bytedance/sonic"
	"github.com/valyala/fasthttp"
)

type FastHTTPServer struct {
	paymentService *service.PaymentService
	queueService   *service.QueueService
	config         *config.Config
}

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	redisClient := redis.NewClientWithConfig(cfg.GetRedisAddr(), cfg.IsRedisUDS())
	if err := redisClient.Ping(context.Background()); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	httpClient := client.NewHTTPClient()
	circuitBreaker := health.NewChecker(httpClient, redisClient, cfg)
	queueService := service.NewQueueService(cfg)
	paymentRepo := repository.NewPaymentRepository(httpClient, redisClient, cfg)
	paymentService := service.NewPaymentService(circuitBreaker, queueService, paymentRepo, cfg)

	queueService.SetPaymentProcessor(paymentService.ProcessPayment)

	server := &FastHTTPServer{
		paymentService: paymentService,
		queueService:   queueService,
		config:         cfg,
	}

	socketPath := os.Getenv("SOCKET_PATH")
	if socketPath == "" {
		socketPath = fmt.Sprintf("/var/run/sockets/app%s.sock", cfg.AppName)
	}

	os.Remove(socketPath)
	os.MkdirAll("/var/run/sockets", 0755)
	os.MkdirAll("/var/run/health", 0755)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Failed to create Unix socket: %v", err)
	}
	defer listener.Close()

	if err := os.Chmod(socketPath, 0666); err != nil {
		log.Fatalf("Failed to set socket permissions: %v", err)
	}

	fastHTTPServer := &fasthttp.Server{
		Handler:            server.handler,
		ReadTimeout:        500 * time.Millisecond,
		WriteTimeout:       500 * time.Millisecond,
		IdleTimeout:        30 * time.Second,
		DisableKeepalive:   false,
		MaxRequestBodySize: 1024,
		TCPKeepalive:       true,
	}

	circuitBreaker.Start()
	queueService.Start()

	go func() {
		if err := fastHTTPServer.Serve(listener); err != nil {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	circuitBreaker.Stop()
	queueService.Stop()

	if err := fastHTTPServer.Shutdown(); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	os.Remove(socketPath)
	redisClient.Close()
}


func (s *FastHTTPServer) handler(ctx *fasthttp.RequestCtx) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if elapsed >= 20*time.Microsecond && ctx.Method()[0] == 'P' {
			log.Printf("[PERF]: Add took: %v", elapsed)
		}
	}()

	path := utils.UnsafeString(ctx.Path())

	if path == "/payments" {
		body := ctx.Request.SwapBody(nil)
		s.queueService.Add(body)

		ctx.SetStatusCode(fasthttp.StatusCreated)
		return
	}

	if path == "/payments-summary" {
		s.handlePaymentsSummary(ctx)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
}

func (s *FastHTTPServer) handlePaymentsSummary(ctx *fasthttp.RequestCtx) {
	time.Sleep(500 * time.Millisecond)
	var fromTime, toTime *int64

	if fromBytes := ctx.QueryArgs().Peek("from"); fromBytes != nil {
		if from, err := time.Parse(time.RFC3339, string(fromBytes)); err == nil {
			timestamp := from.UnixMilli()
			fromTime = &timestamp
		}
	}

	if toBytes := ctx.QueryArgs().Peek("to"); toBytes != nil {
		if to, err := time.Parse(time.RFC3339, string(toBytes)); err == nil {
			timestamp := to.UnixMilli()
			toTime = &timestamp
		}
	}
	result, err := s.paymentService.GetPaymentSummary(fromTime, toTime)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}

	responseData, err := sonic.ConfigFastest.Marshal(result)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetContentType("application/json")
	ctx.Write(responseData)
}
