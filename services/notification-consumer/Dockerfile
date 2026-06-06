FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/notification-consumer ./services/notification-consumer/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/notification-consumer /notification-consumer
ENTRYPOINT ["/notification-consumer"]
