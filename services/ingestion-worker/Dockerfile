
FROM golang:1.27.2-alpine AS builder

WORKDIR /workspace

COPY . .

WORKDIR /workspace/services/ingestion-worker

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/ingestion-worker .

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /out/ingestion-worker ./ingestion-worker

RUN mkdir -p /app/uploads

CMD ["./ingestion-worker"]
