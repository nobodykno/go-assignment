# Build stage
FROM golang:1.27.2-alpine AS builder

WORKDIR /src


COPY go.work go.work.sum ./
COPY apps/api/go.* ./apps/api/
COPY packages/go.* ./packages/
COPY services/ingestion/go.* ./services/ingestion/
COPY services/ingestion-worker/go.* ./services/ingestion-worker/
COPY services/validation/go.* ./services/validation/
COPY services/transformation/go.* ./services/transformation/
COPY services/aggregation/go.* ./services/aggregation/
COPY services/export/go.* ./services/export/

RUN cd apps/api && go mod download

# Copy the source code and build a static, stripped binary.
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/api ./apps/api

# Runtime stage
FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S -g 1001 app && \
    adduser -S -D -H -u 1001 -G app app

WORKDIR /app

COPY --from=builder /out/api /app/api

# Writable data directories owned by the non-root user.
RUN mkdir -p /app/uploads && chown app:app /app/uploads

EXPOSE 8080

USER app:app

ENTRYPOINT ["/app/api"]
