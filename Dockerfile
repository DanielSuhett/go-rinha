FROM golang:1.23-alpine AS builder

RUN apk add --no-cache upx

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags="-w -s -extldflags '-static'" \
    -gcflags="all=-N -l -B -C" \
    -tags="netgo osusergo" \
    -trimpath \
    -a -installsuffix cgo \
    -o main ./cmd/server && \
    upx --best --lzma main

FROM scratch AS runtime

COPY --from=alpine:latest /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /app/main /main

USER 1001:1001

EXPOSE 8080

ENV GOMEMLIMIT=90MiB \
    GOMAXPROCS=1 \
    GODEBUG=gctrace=0,schedtrace=0,scheddetail=0 \
    GOTRACEBACK=none \
    GOGCCFLAGS="-m64 -march=native -mtune=native" \
    MALLOC_ARENA_MAX=1

ENTRYPOINT ["/main"]