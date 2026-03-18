FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/worker ./cmd/worker

FROM alpine:3.22
RUN adduser -D -H -u 10001 appuser
USER appuser

COPY --from=builder /bin/worker /worker
EXPOSE 8081
ENTRYPOINT ["/worker"]
