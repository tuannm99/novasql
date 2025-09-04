package storage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tuannm99/novasql/internal/storage"
)

var (
	defaultPageSize int = storage.PageSize
	defaultPageID       = 0
	defaultUpper        = 8192
	defaultLower        = 24
	defaultNumSlots     = 0
)

func TestNewPage_init(t *testing.T) {
	buf := make([]byte, defaultPageSize)
	p := storage.NewPage(buf, uint32(defaultPageID))

	require.Equal(t, defaultUpper, p.Upper())
	require.Equal(t, defaultLower, p.Lower())
	require.Equal(t, defaultNumSlots, p.NumSlots())
}
