package volume

import (
	"context"
	"io"
	"os"
)

// This file holds the write path's cancellation and durability plumbing: the
// context-aware payload copy and the directory fsync that makes a new file name
// durable. Both are shared by WriteFile and the cross-device move fallback.
//
// The volume deliberately keeps no write-path statistics: the counters had no
// production reader, so the write fast path no longer pays for them.

// contextReader reports the context error instead of reading more content once
// the request context is done, so a cancelled write stops at the next chunk
// rather than streaming the whole payload.
type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

// Read implements io.Reader.
//
// The context is checked after the read as well as before it. A cancellation
// that lands during the final chunk would otherwise be invisible: the reader
// returns (n, nil) or (n, io.EOF) and the copy would report success even though
// the request was already abandoned.
func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	n, err := r.reader.Read(p)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}

	return n, err
}

// syncDirectory flushes a directory's entries so a newly created file name is
// durable, not just its contents.
//
// It is best-effort by contract: the caller treats a failure as a lost
// optimization, because opening a directory for fsync is not portable (Windows
// rejects os.Open on a directory), and the file contents were already synced.
func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()

	return directory.Sync()
}
