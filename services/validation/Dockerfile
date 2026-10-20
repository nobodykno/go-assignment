
FROM golang:1.27.2-alpine AS builder

WORKDIR /workspace

COPY . .

WORKDIR /workspace/services/validation

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/validation .

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /out/validation ./validation

EXPOSE 8081

CMD ["./validation"]
