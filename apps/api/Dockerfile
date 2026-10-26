
# Build stage
FROM golang:1.27.2-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git ca-certificates

# Copy the Go workspace manifests.
COPY go.work go.work.sum ./

# Copy module manifests first for dependency caching.
COPY apps/api/go.mod apps/api/go.sum ./apps/api/
COPY packages/go.mod packages/go.sum ./packages/

COPY services/ingestion/go.mod ./services/ingestion/
COPY services/ingestion-worker/go.mod ./services/ingestion-worker/
COPY services/validation/go.mod ./services/validation/
COPY services/transformation/go.mod ./services/transformation/
COPY services/aggregation/go.mod ./services/aggregation/
COPY services/export/go.mod ./services/export/

# Copy all workspace module source code.
COPY apps/api ./apps/api
COPY packages ./packages
COPY services/ingestion ./services/ingestion
COPY services/ingestion-worker ./services/ingestion-worker
COPY services/validation ./services/validation
COPY services/transformation ./services/transformation
COPY services/aggregation ./services/aggregation
COPY services/export ./services/export

# Download dependencies and build the API.
WORKDIR /app/apps/api

RUN go mod download

RUN mkdir -p /app/bin && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/bin/api .

# Runtime stage
FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/bin/api ./api

RUN mkdir -p /app/uploads

EXPOSE 8080

CMD ["./api"]
