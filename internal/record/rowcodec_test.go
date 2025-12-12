package record

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// makeTestSchema builds a simple schema used across tests.
func makeTestSchema() Schema {
	return Schema{
		Cols: []Column{
			{Name: "id32", Type: ColInt32, Nullable: false},
			{Name: "id64", Type: ColInt64, Nullable: false},
			{Name: "active", Type: ColBool, Nullable: false},
			{Name: "score", Type: ColFloat64, Nullable: false},
			{Name: "name", Type: ColText, Nullable: true},
			{Name: "blob", Type: ColBytes, Nullable: true},
		},
	}
}

func TestEncodeDecodeRow_RoundTrip(t *testing.T) {
	schema := makeTestSchema()

	values := []any{
		int32(42),                // id32
		int64(123456789),         // id64
		true,                     // active
		3.14159,                  // score
		"hello",                  // name
		[]byte{0x01, 0x02, 0x03}, // blob
	}

	buf, err := EncodeRow(schema, values)
	require.NoError(t, err)
	require.NotEmpty(t, buf)

	row, err := DecodeRow(schema, buf)
	require.NoError(t, err)

	require.Len(t, row, len(values))
	require.Equal(t, int32(42), row[0].(int32))
	require.Equal(t, int64(123456789), row[1].(int64))
	require.True(t, row[2].(bool))

	// Float comparison with small epsilon
	require.InDelta(t, 3.14159, row[3].(float64), 1e-9)

	require.Equal(t, "hello", row[4].(string))
	require.Equal(t, []byte{0x01, 0x02, 0x03}, row[5].([]byte))
}

func TestEncodeDecodeRow_Nullable(t *testing.T) {
	schema := makeTestSchema()

	// Set nullable TEXT and BYTES to nil
	values := []any{
		int32(1),
		int64(2),
		false,
		1.5,
		nil, // name
		nil, // blob
	}

	buf, err := EncodeRow(schema, values)
	require.NoError(t, err)

	row, err := DecodeRow(schema, buf)
	require.NoError(t, err)

	require.Nil(t, row[4]) // name
	require.Nil(t, row[5]) // blob
}

func TestEncodeRow_SchemaMismatch(t *testing.T) {
	schema := makeTestSchema()

	t.Run("wrong number of values", func(t *testing.T) {
		_, err := EncodeRow(schema, []any{1, 2, 3}) // fewer than NumCols
		require.Error(t, err)
		require.ErrorIs(t, err, ErrSchemaMismatch)
	})

	t.Run("non-nullable column is nil", func(t *testing.T) {
		values := []any{
			nil,            // id32 is not nullable
			int64(1),       // id64
			true,           // active
			1.0,            // score
			"ok",           // name
			[]byte("abcd"), // blob
		}
		_, err := EncodeRow(schema, values)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrSchemaMismatchNotAllowNull)
	})

	t.Run("wrong type for column", func(t *testing.T) {
		// id32 should be an integer but we pass string
		values := []any{
			"not-int32",    // id32
			int64(1),       // id64
			true,           // active
			1.0,            // score
			"ok",           // name
			[]byte("abcd"), // blob
		}
		_, err := EncodeRow(schema, values)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrSchemaMismatchNotInt32)
	})
}

func TestEncodeRow_VarTooLong(t *testing.T) {
	schema := Schema{
		Cols: []Column{
			{Name: "name", Type: ColText, Nullable: false},
		},
	}

	// Create a string longer than MaxUint16
	longStr := strings.Repeat("a", math.MaxUint16+1)

	_, err := EncodeRow(schema, []any{longStr})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrVarTooLong)
}

func TestDecodeRow_BadBuffer(t *testing.T) {
	schema := makeTestSchema()

	values := []any{
		int32(42),
		int64(99),
		true,
		2.71828,
		"test",
		[]byte{0xAA, 0xBB},
	}

	buf, err := EncodeRow(schema, values)
	require.NoError(t, err)

	t.Run("truncated buffer", func(t *testing.T) {
		// Cut some bytes from the end
		truncated := buf[:len(buf)-3]
		_, err := DecodeRow(schema, truncated)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBadBuffer)
	})

	t.Run("too short for nullmap", func(t *testing.T) {
		// Provide a buffer shorter than null bitmap
		_, err := DecodeRow(schema, []byte{0x00})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrBadBuffer)
	})
}
