package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
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
)

type UDSServer struct {
	paymentService *service.PaymentService
	queueService   *service.QueueService
	config         *config.Config
}

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

	server := &UDSServer{
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

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Failed to create Unix socket: %v", err)
	}
	defer listener.Close()

	if err := os.Chmod(socketPath, 0666); err != nil {
		log.Fatalf("Failed to set socket permissions: %v", err)
	}

	httpServer := &http.Server{
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
		IdleTimeout:  30 * time.Second,
		Handler:      server.setupRoutes(),
	}

	circuitBreaker.Start()
	queueService.Start()

	go func() {
		log.Printf("Server listening on Unix socket: %s", socketPath)
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	circuitBreaker.Stop()
	queueService.Stop()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	os.Remove(socketPath)
	redisClient.Close()
	log.Println("Server stopped")
}

func (s *UDSServer) setupRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/payments", s.handlePayments)
	mux.HandleFunc("/payments-summary", s.handlePaymentsSummary)
	mux.HandleFunc("/health", s.handleHealth)
	return mux
}

func (s *UDSServer) handlePayments(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	body, _ := io.ReadAll(r.Body)
	go func(r []byte) {
		s.queueService.Add(string(body))
	}(body)

	w.WriteHeader(http.StatusCreated)
}

func (s *UDSServer) handlePaymentsSummary(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	var fromTime, toTime *int64

	if fromStr := query.Get("from"); fromStr != "" {
		if from, err := time.Parse(time.RFC3339, fromStr); err == nil {
			timestamp := from.UnixMilli()
			fromTime = &timestamp
		}
	}

	if toStr := query.Get("to"); toStr != "" {
		if to, err := time.Parse(time.RFC3339, toStr); err == nil {
			timestamp := to.UnixMilli()
			toTime = &timestamp
		}
	}

	result, err := s.paymentService.GetPaymentSummary(fromTime, toTime)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (s *UDSServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}
