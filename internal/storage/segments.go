package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// SegFileName returns segment file name:
//   - seg 0: base
//   - seg N>0: base.N
func SegFileName(base string, segNo int32) string {
	if segNo <= 0 {
		return base
	}
	return fmt.Sprintf("%s.%d", base, segNo)
}

// RemoveAllSegments removes Base, Base.1, Base.2, ... until first missing.
// This follows the same convention as CountPagesLocalFileSet.
func RemoveAllSegments(lfs LocalFileSet) error {
	for segNo := int32(0); ; segNo++ {
		path := filepath.Join(lfs.Dir, SegFileName(lfs.Base, segNo))
		err := os.Remove(path)
		if err == nil {
			continue
		}
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		return err
	}
	return nil
}

// RenameAllSegments renames Base, Base.1, Base.2, ... until first missing.
func RenameAllSegments(oldLFS, newLFS LocalFileSet) error {
	// Ensure new dir exists (old dir existence is checked by Stat below anyway).
	if err := os.MkdirAll(newLFS.Dir, 0o755); err != nil {
		return err
	}

	for segNo := int32(0); ; segNo++ {
		oldPath := filepath.Join(oldLFS.Dir, SegFileName(oldLFS.Base, segNo))
		newPath := filepath.Join(newLFS.Dir, SegFileName(newLFS.Base, segNo))

		if _, err := os.Stat(oldPath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				break
			}
			return err
		}
		if err := os.Rename(oldPath, newPath); err != nil {
			return err
		}
	}
	return nil
}

func FsKeyOf(fs FileSet) (string, LocalFileSet, bool) {
	lfs, ok := fs.(LocalFileSet)
	if !ok {
		return "", LocalFileSet{}, false
	}
	return lfs.Dir + "|" + lfs.Base, lfs, true
}
