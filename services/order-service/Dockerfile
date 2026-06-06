FROM golang:1.26-alpine AS builder
WORKDIR /app
# Layer the dependency download separately so it's cached unless go.mod changes.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/order-service ./services/order-service/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/order-service /order-service
EXPOSE 8080
ENTRYPOINT ["/order-service"]
