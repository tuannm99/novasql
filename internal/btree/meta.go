package btree

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/tuannm99/novasql/internal/storage"
)

const (
	metaFileSuffix = ".btree.meta.json"
	metaVersion    = 1
)

type diskMeta struct {
	Version    int    `json:"version"`
	Root       uint32 `json:"root"`
	Height     int    `json:"height"`
	NextPageID uint32 `json:"next_page_id"`
}

func metaPathForFileSet(fs storage.FileSet) (string, bool) {
	lfs, ok := fs.(storage.LocalFileSet)
	if !ok {
		return "", false
	}
	// meta file nằm cạnh các segment: <Dir>/<Base>.btree.meta.json
	return filepath.Join(lfs.Dir, lfs.Base+metaFileSuffix), true
}

func (t *Tree) loadMeta() (diskMeta, bool, error) {
	if !t.metaEnabled || t.metaPath == "" {
		return diskMeta{}, false, nil
	}

	data, err := os.ReadFile(t.metaPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return diskMeta{}, false, nil
		}
		return diskMeta{}, false, err
	}

	var m diskMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return diskMeta{}, false, err
	}
	if m.Version <= 0 {
		// backward/unknown -> still accept
		m.Version = metaVersion
	}
	return m, true, nil
}

func (t *Tree) saveMeta() error {
	if !t.metaEnabled || t.metaPath == "" {
		return nil
	}

	m := diskMeta{
		Version:    metaVersion,
		Root:       t.Root,
		Height:     t.Height,
		NextPageID: t.nextPageID,
	}

	data, err := json.MarshalIndent(&m, "", "  ")
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(t.metaPath), 0o755); err != nil {
		return err
	}

	if err := writeFileAtomic(t.metaPath, data, 0o644); err != nil {
		return err
	}

	slog.Debug("btree.meta.saved",
		"path", t.metaPath,
		"root", m.Root,
		"height", m.Height,
		"nextPageID", m.NextPageID,
	)
	return nil
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	tmp, err := os.CreateTemp(dir, base+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("atomic rename: %w", err)
	}

	ok = true
	return nil
}
