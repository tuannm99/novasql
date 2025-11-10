package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	// "github.com/tuannm99/novasql/internal/storage"
)

var (
	defaultPageSize int = PageSize
	defaultPageID       = 0
	defaultUpper        = 8192
	defaultLower        = 12
	defaultNumSlots     = 0
)

func TestNewPage_init(t *testing.T) {
	buf := make([]byte, defaultPageSize)
	p, err := NewPage(buf, uint32(defaultPageID))

	require.NoError(t, err)
	t.Log(p.Buf)
	require.Equal(t, uint16(defaultUpper), p.upper())
	require.Equal(t, uint16(defaultLower), p.lower())
	require.Equal(t, defaultNumSlots, p.NumSlots())
}
