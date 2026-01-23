# Venue - Multi-Tenant File Storage Queue System

[![Go Version](https://img.shields.io/badge/Go-1.25%2B-blue)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Venue is a high-performance, multi-tenant file storage pool system for Go, designed as a **file queue system** with automatic retry and queue-based processing.

> This is a Go port of [Locus](https://github.com/cocosip/Locus) (.NET/C#)

## Key Features

📦 **Multi-Tenant Storage**
- Complete tenant isolation with per-tenant databases
- Enable/disable tenant controls
- Auto-tenant creation support

🔄 **File Queue Processing**
- System-generated file keys (UUID)
- Queue-based retrieval (FIFO)
- Automatic retry with exponential backoff
- Thread-safe file allocation

📁 **Storage Management**
- Multiple storage volumes
- Automatic volume selection
- Health monitoring
- Directory-level quota control

🧹 **Automatic Cleanup**
- Empty directory cleanup
- Timeout detection and reset
- Orphaned file cleanup
- Failed file retention policies

## Quick Start

### Installation

```bash
go get github.com/cocosip/venue
```

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "io"
    "strings"

    "github.com/cocosip/venue/pkg/core"
)

func main() {
    ctx := context.Background()

    // Create storage pool (implementation in progress)
    // pool := storage.NewStoragePool(options)

    // Get tenant
    tenant := core.TenantContext{
        ID:     "tenant-001",
        Status: core.TenantStatusEnabled,
    }

    // Write a file - system generates fileKey
    content := strings.NewReader("Hello, Venue!")
    originalName := "document.txt"
    fileKey, err := pool.WriteFile(ctx, tenant, content, &originalName)
    if err != nil {
        panic(err)
    }
    fmt.Printf("File written with key: %s\n", fileKey)

    // Process files from queue
    for {
        file, err := pool.GetNextFileForProcessing(ctx, tenant)
        if err != nil {
            break
        }

        // Read and process file
        stream, _ := pool.ReadFile(ctx, tenant, file.FileKey)
        processFile(stream)
        stream.Close()

        // Mark as completed (deletes file)
        pool.MarkAsCompleted(ctx, file.FileKey)
    }
}

func processFile(r io.Reader) {
    // Your processing logic here
}
```

## Architecture

```
┌─────────────────────────────────────────────┐
│   API Layer (StoragePool)                   │
│   - Unified storage + queue interface       │
├─────────────────────────────────────────────┤
│   Active-Data Cache (Per-Tenant)           │
│   - Only Pending/Processing/Failed files    │
├─────────────────────────────────────────────┤
│   Persistence Layer (Per-Tenant SQLite)    │
│   - MetadataRepository: File metadata       │
│   - DirectoryQuotaRepository: Quota limits  │
├─────────────────────────────────────────────┤
│   Tenant Management (JSON + Cache)         │
│   - TenantMetadata: Status, dates           │
├─────────────────────────────────────────────┤
│   Storage Volumes (LocalFileSystemVolume)  │
│   - Configurable sharding                   │
└─────────────────────────────────────────────┘
```

## Development Status

**Current Phase**: Foundation (Phase 1) ⏳

- ✅ Requirements defined
- ✅ Core interfaces defined
- ✅ Core models defined
- ✅ Custom errors defined
- ⏳ Implementation in progress

See [REQUIREMENTS_AND_PLAN.md](doc/REQUIREMENTS_AND_PLAN.md) for the complete development roadmap.

## Build and Test

```bash
# Build all packages
go build ./...

# Run all tests
go test ./...

# Run tests with race detector
go test -race ./...

# Run tests with coverage
go test -cover ./...

# Run benchmarks
go test -bench=. -benchmem ./...
```

## Documentation

- [CLAUDE.md](CLAUDE.md) - Development guide for Claude Code
- [REQUIREMENTS_AND_PLAN.md](doc/REQUIREMENTS_AND_PLAN.md) - Detailed requirements and 13-week development plan
- [Locus README](doc/README.md) - Reference C# implementation documentation

## Project Structure

```
venue/
├── pkg/
│   ├── core/           # Core abstractions and interfaces
│   ├── tenant/         # Tenant management
│   ├── storage/        # Storage pool implementation
│   ├── volume/         # Storage volumes
│   └── cleanup/        # Cleanup services
├── internal/
│   └── config/         # Configuration
├── test/
│   ├── unit/           # Unit tests
│   ├── integration/    # Integration tests
│   └── benchmark/      # Benchmarks
├── examples/           # Example applications
└── cmd/                # Command-line tools
```

## Contributing

This project is currently in active development. Contributions are welcome once the core implementation is complete.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Reference

Based on [Locus](https://github.com/cocosip/Locus) by cocosip.
