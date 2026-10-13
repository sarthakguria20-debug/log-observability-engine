FROM golang:1.22-alpine AS builder

WORKDIR /src
COPY go.mod ./
COPY cmd/ ./cmd/
COPY internal/ ./internal/

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/ingestor ./cmd/ingestor && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/log-agent ./cmd/log-agent

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /out/ingestor /app/ingestor
COPY --from=builder /out/log-agent /app/log-agent
COPY dashboard/ /app/dashboard/
COPY clickhouse/ /app/clickhouse/

EXPOSE 8080
ENTRYPOINT ["/app/ingestor"]

