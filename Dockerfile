# Multi-stage build for Venue storage pool system

# Build stage
FROM golang:1.25-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make ca-certificates tzdata

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy source code
COPY . .

# Build the application
# CGO_ENABLED=0 for static binary (BadgerDB works without CGO)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.Version=${VERSION}" \
    -o venue \
    ./venue.go

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 venue && \
    adduser -D -u 1000 -G venue venue

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/venue /app/venue

# Copy example config
COPY --from=builder /build/venue-config-example.yaml /app/venue-config-example.yaml

# Create necessary directories with proper permissions
RUN mkdir -p /app/data /app/config && \
    chown -R venue:venue /app

# Switch to non-root user
USER venue

# Expose ports (if needed for future HTTP API)
# EXPOSE 8080

# Set environment variables
ENV VENUE_CONFIG_PATH=/app/config/venue-config.yaml
ENV VENUE_DATA_PATH=/app/data

# Health check (adjust based on your actual health check implementation)
# HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
#   CMD ["/app/venue", "health"] || exit 1

# Default command
ENTRYPOINT ["/app/venue"]
CMD ["--help"]
