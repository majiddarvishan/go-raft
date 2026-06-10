# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder

# نصب protoc و ابزارهای لازم
RUN apk add --no-cache protobuf protobuf-dev make git

# نصب Go plugins برای protoc
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.1 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.4.0

WORKDIR /build

# ابتدا dependencies (cache بهتر)
COPY go.mod go.sum* ./
RUN go mod download && \
    go mod tidy

# کپی کل پروژه
COPY . .

# تولید کد از proto files
RUN protoc \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    -I . \
    api/proto/auth/*.proto

# Build هر دو binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /client ./cmd/client

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /server .
COPY --from=builder /client .

# Raft data directory
VOLUME ["/data"]

# gRPC port (Raft port داخلی است و expose نمی‌شود)
EXPOSE 9001

ENTRYPOINT ["/app/server"]
