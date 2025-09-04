package storage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tuannm99/novasql/internal/storage"
)

func TestStorageManager(t *testing.T) {
	fs := storage.LocalFileSet{Dir: "../../data/test/base/16384", Base: "12345"}
	sm := storage.NewStorageManager()

	// Load page
	pg, err := sm.LoadPage(fs, 0)
	assert.Nil(t, err)
	assert.NotNil(t, pg)
	assert.IsType(t, &storage.Page{}, pg)
}
