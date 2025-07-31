FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o main ./cmd/server

FROM alpine:latest AS runtime

RUN addgroup -g 1001 -S golang && adduser -S gouser -u 1001 -G golang

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder --chown=gouser:golang /app/main .

USER gouser

EXPOSE 8080

ENV GOMEMLIMIT=120MiB

CMD ["./main"]
