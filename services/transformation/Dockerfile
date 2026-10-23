
FROM golang:1.27.2-alpine AS builder

WORKDIR /workspace

COPY . .

WORKDIR /workspace/services/transformation

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/transformation .

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /out/transformation ./transformation

RUN mkdir -p /app/output

EXPOSE 8082

ENV TRANSFORMATION_PORT=8082
ENV TRANSFORMED_OUTPUT_DIR=/app/output

CMD ["./transformation"]
