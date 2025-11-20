package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOverflow_WriteRead_RoundTrip(t *testing.T) {
	t.Parallel()

	// Use temp dir for overflow segments.
	dir := t.TempDir()
	fs := LocalFileSet{
		Dir:  dir,
		Base: "ovf_test",
	}

	sm := NewStorageManager()
	ovf := NewOverflowManager(sm, fs)

	// Payload bigger than one overflow page to force multi-page chain.
	// PageSize = 8192, header ~8 bytes, available ~8184.
	// Chọn 12012 giống manual test.
	payloadLen := 12012
	payload := bytes.Repeat([]byte("X"), payloadLen)

	ref, err := ovf.Write(payload)
	require.NoError(t, err)

	// FirstPageID *có thể* là 0, nên không assert NotZero ở đây.
	// Điều quan trọng là Length đúng và Read trả về đúng dữ liệu.
	require.Equal(t, uint32(len(payload)), ref.Length)

	out, err := ovf.Read(ref)
	require.NoError(t, err)
	require.Equal(t, payload, out)
}
