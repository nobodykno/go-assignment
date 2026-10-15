
# Build stage
FROM golang:1.27.2-alpine AS builder

WORKDIR /app

# Copy workspace and module manifests first
COPY go.work ./
COPY apps/api/go.mod ./apps/api/go.mod
COPY packages/go.mod ./packages/go.mod

# Copy source code for the API and shared package
COPY apps/api ./apps/api
COPY packages ./packages

WORKDIR /app/apps/api

RUN go mod download
RUN CGO_ENABLED=0 go build -o /app/bin/api .

# Runtime stage
FROM alpine:3.22

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/bin/api ./api

EXPOSE 8080

CMD ["./api"]
