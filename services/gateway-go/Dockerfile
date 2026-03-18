FROM golang:1.25-alpine AS builder
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/gateway ./cmd/server

FROM alpine:3.22
WORKDIR /app
COPY --from=builder /out/gateway /app/gateway

EXPOSE 8080
ENV GATEWAY_ADDR=:8080
CMD ["/app/gateway"]
