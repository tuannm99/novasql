package storage

import (
	"fmt"
	"math"
)

// WALWriter adapts StorageManager to wal.PageWriter without creating import cycle.
// (wal package must not import storage)
type WALWriter struct {
	SM *StorageManager
}

func NewWALWriter(sm *StorageManager) *WALWriter {
	return &WALWriter{SM: sm}
}

func (w *WALWriter) WritePage(dir, base string, pageID uint32, pageBytes []byte) error {
	if w == nil || w.SM == nil {
		return nil
	}
	if pageID > math.MaxInt32 {
		return fmt.Errorf("storage: pageID overflow: %d", pageID)
	}
	fs := LocalFileSet{Dir: dir, Base: base}
	return w.SM.WritePage(fs, int32(pageID), pageBytes)
}
