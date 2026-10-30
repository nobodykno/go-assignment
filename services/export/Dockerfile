
FROM golang:1.27-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY . .

RUN mkdir -p /out && \
    go build -trimpath -ldflags="-s -w" \
    -o /out/export ./services/export

FROM alpine:3.22

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /out/export /app/export

ENV EXPORTED_OUTPUT_DIR=/app/output

RUN mkdir -p /app/output

ENTRYPOINT ["/app/export"]
