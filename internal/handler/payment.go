package handler

import (
	"encoding/json"
	"go-rinha/internal/service"
	"time"

	"github.com/valyala/fasthttp"
)

type PaymentHandler struct {
	paymentService *service.PaymentService
	queueService   *service.QueueService
}

func NewPaymentHandler(paymentService *service.PaymentService, queueService *service.QueueService) *PaymentHandler {
	return &PaymentHandler{
		paymentService: paymentService,
		queueService:   queueService,
	}
}

func (h *PaymentHandler) PostPayments(ctx *fasthttp.RequestCtx) {
	var payment map[string]interface{}
	if err := json.Unmarshal(ctx.PostBody(), &payment); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}
	
	h.queueService.Add(string(ctx.PostBody()))
	ctx.SetStatusCode(fasthttp.StatusCreated)
}

func (h *PaymentHandler) GetPaymentsSummary(ctx *fasthttp.RequestCtx) {
	args := ctx.QueryArgs()

	var fromTime, toTime *int64

	if fromStr := string(args.Peek("from")); fromStr != "" {
		if from, err := time.Parse(time.RFC3339, fromStr); err == nil {
			timestamp := from.UnixMilli()
			fromTime = &timestamp
		}
	}

	if toStr := string(args.Peek("to")); toStr != "" {
		if to, err := time.Parse(time.RFC3339, toStr); err == nil {
			timestamp := to.UnixMilli()
			toTime = &timestamp
		}
	}

	result, err := h.paymentService.GetPaymentSummary(fromTime, toTime)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}

	response, err := json.Marshal(result)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetContentType("application/json")
	ctx.SetBody(response)
}

func (h *PaymentHandler) GetHealth(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBodyString("OK")
}

