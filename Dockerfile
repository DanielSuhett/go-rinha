FROM golang:1.23-alpine AS builder

RUN apk add --no-cache upx

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags="-w -s" \
    -o main ./cmd/server && \
    upx --best --lzma main

FROM alpine:latest AS runtime

RUN apk add --no-cache ca-certificates

RUN mkdir -p /var/run/sockets && chmod 755 /var/run/sockets

COPY --from=builder /app/main /main

RUN chmod +x /main

ENV SOCKET_PATH=/var/run/sockets/app.sock \
    GOMEMLIMIT=90MiB

ENTRYPOINT ["/main"]