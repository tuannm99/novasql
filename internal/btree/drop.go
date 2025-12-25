package btree

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tuannm99/novasql/internal/storage"
)

// DropIndex removes all index segments and its meta file.
// Works for LocalFileSet only.
func DropIndex(lfs storage.LocalFileSet) error {
	// Ensure directory exists; Drop should be idempotent.
	if err := os.MkdirAll(lfs.Dir, 0o755); err != nil {
		return err
	}

	// Remove page segments: Base, Base.1, ...
	if err := storage.RemoveAllSegments(lfs); err != nil {
		return err
	}

	// Remove meta file: <Base>.btree.meta.json (if you use meta persistence)
	metaPath := filepath.Join(lfs.Dir, lfs.Base+metaFileSuffix)
	if err := os.Remove(metaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}

func dropIndexFileSet(fs storage.FileSet) error {
	lfs, ok := fs.(storage.LocalFileSet)
	if !ok {
		return fmt.Errorf("btree: DropIndex only supports LocalFileSet for now")
	}
	return DropIndex(lfs)
}

