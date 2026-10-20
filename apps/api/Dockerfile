
# Build stage
FROM golang:1.27.2-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git ca-certificates

# Copy the workspace manifest
COPY go.work ./

# Copy all module manifests
COPY apps/api/go.mod ./apps/api/go.mod
COPY packages/go.mod ./packages/go.mod
COPY services/ingestion/go.mod ./services/ingestion/go.mod
COPY services/ingestion-worker/go.mod ./services/ingestion-worker/go.mod
COPY services/validation/go.mod ./services/validation/go.mod

# Copy source code for all workspace modules
COPY apps/api ./apps/api
COPY packages ./packages
COPY services/ingestion ./services/ingestion
COPY services/ingestion-worker ./services/ingestion-worker
COPY services/validation ./services/validation

WORKDIR /app/apps/api

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/api .

# Runtime stage
FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/bin/api ./api

RUN mkdir -p /app/uploads

EXPOSE 8080

CMD ["./api"]
