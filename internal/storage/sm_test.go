package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageManager(t *testing.T) {
	fs := LocalFileSet{Dir: "../../data/test/base", Base: "segment"}
	sm := NewStorageManager()

	// Load page
	pg, err := sm.LoadPage(fs, 0)
	require.NoError(t, err)
	assert.NotNil(t, pg)
	assert.IsType(t, &Page{}, pg)
}
