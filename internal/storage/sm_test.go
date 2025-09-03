package storage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tuannm99/novasql/internal/storage"
)

func TestStorageManager(t *testing.T) {
	// tạo SM + FileSet cho quan hệ "12345" trong thư mục "data/base/16384"
	sm := storage.NewStorageManager("data")
	fs := storage.LocalFileSet{Dir: "data/base/16384", Base: "12345"}

	// Load page
	pg, err := sm.LoadPage(fs, 0)
	assert.NotNil(t, err)
	assert.NotNil(t, pg)
}
