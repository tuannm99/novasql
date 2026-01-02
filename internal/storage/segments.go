package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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

// listSegmentsLocal scans lfs.Dir and returns all segment numbers for lfs.Base.
// It matches: Base and Base.<int>.
func listSegmentsLocal(lfs LocalFileSet) ([]int32, error) {
	ents, err := os.ReadDir(lfs.Dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	segs := make([]int32, 0)
	prefix := lfs.Base + "."

	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if name == lfs.Base {
			segs = append(segs, 0)
			continue
		}
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		suf := strings.TrimPrefix(name, prefix)
		n64, err := strconv.ParseInt(suf, 10, 32)
		if err != nil || n64 <= 0 {
			continue
		}
		segs = append(segs, int32(n64))
	}

	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })
	return segs, nil
}

// RemoveAllSegments removes Base, Base.1, Base.2, ... (robust: scan dir).
func RemoveAllSegments(lfs LocalFileSet) error {
	segs, err := listSegmentsLocal(lfs)
	if err != nil {
		return err
	}
	for _, segNo := range segs {
		path := filepath.Join(lfs.Dir, SegFileName(lfs.Base, segNo))
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// RenameAllSegments renames Base, Base.1, Base.2, ... (robust: scan dir).
func RenameAllSegments(oldLFS, newLFS LocalFileSet) error {
	if err := os.MkdirAll(newLFS.Dir, 0o755); err != nil {
		return err
	}

	segs, err := listSegmentsLocal(oldLFS)
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		return nil
	}

	// Pre-check to avoid overwriting existing targets.
	for _, segNo := range segs {
		newPath := filepath.Join(newLFS.Dir, SegFileName(newLFS.Base, segNo))
		if _, err := os.Stat(newPath); err == nil {
			return fmt.Errorf("rename segments: target exists: %s", newPath)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	for _, segNo := range segs {
		oldPath := filepath.Join(oldLFS.Dir, SegFileName(oldLFS.Base, segNo))
		newPath := filepath.Join(newLFS.Dir, SegFileName(newLFS.Base, segNo))
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
	// Normalize dir so cache key stable.
	dir := filepath.Clean(lfs.Dir)
	return dir + "|" + lfs.Base, LocalFileSet{Dir: dir, Base: lfs.Base}, true
}
