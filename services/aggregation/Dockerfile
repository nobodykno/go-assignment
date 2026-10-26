
# Build stage
FROM golang:1.27-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates

# Copy the monorepo, including go.work and its local modules.
COPY . .

# Build the aggregation service.
RUN mkdir -p /out && \
    go build -trimpath -ldflags="-s -w" \
    -o /out/aggregation ./services/aggregation

# Runtime stage
FROM alpine:3.22

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /out/aggregation /app/aggregation

ENV AGGREGATED_OUTPUT_DIR=/app/output

RUN mkdir -p /app/output

ENTRYPOINT ["/app/aggregation"]
