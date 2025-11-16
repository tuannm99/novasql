package heap

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/storage"
)

// newTestHeapPage creates an empty page + schema for HeapPage tests.
func newTestHeapPage(t *testing.T) HeapPage {
	t.Helper()

	buf := make([]byte, storage.PageSize)
	p, err := storage.NewPage(buf, 0)
	require.NoError(t, err)

	schema := record.Schema{
		Cols: []record.Column{
			{Name: "id", Type: record.ColInt64, Nullable: false},
			{Name: "name", Type: record.ColText, Nullable: false},
			{Name: "active", Type: record.ColBool, Nullable: false},
		},
	}

	return HeapPage{
		Page:   p,
		Schema: schema,
	}
}

func TestHeapPage_InsertAndRead(t *testing.T) {
	hp := newTestHeapPage(t)

	values := []any{
		int64(1),
		"user-1",
		true,
	}

	slot, err := hp.InsertRow(values)
	require.NoError(t, err)
	require.Equal(t, 0, slot)

	row, err := hp.ReadRow(slot)
	require.NoError(t, err)

	require.Len(t, row, 3)
	require.Equal(t, int64(1), row[0].(int64))
	require.Equal(t, "user-1", row[1].(string))
	require.Equal(t, true, row[2].(bool))
}

func TestHeapPage_Insert_InvalidValues(t *testing.T) {
	hp := newTestHeapPage(t)

	// Wrong number of columns
	_, err := hp.InsertRow([]any{int64(1), "user-1"})
	require.Error(t, err)

	// Wrong type for a column (name should be TEXT/string)
	_, err = hp.InsertRow([]any{int64(1), 12345, true})
	require.Error(t, err)
}

func TestHeapPage_UpdateRow_ShrinkAndGrowRedirect(t *testing.T) {
	hp := newTestHeapPage(t)

	// Insert initial row
	values := []any{
		int64(1),
		"user-1",
		true,
	}
	slot, err := hp.InsertRow(values)
	require.NoError(t, err)
	require.Equal(t, 0, slot)

	// 1) Shrink update: shorter name, should be in-place
	err = hp.UpdateRow(slot, []any{
		int64(1),
		"u1",
		false,
	})
	require.NoError(t, err)

	row, err := hp.ReadRow(slot)
	require.NoError(t, err)
	require.Equal(t, int64(1), row[0].(int64))
	require.Equal(t, "u1", row[1].(string))
	require.Equal(t, false, row[2].(bool))

	// 2) Grow update: much longer name to trigger redirect in Page.UpdateTuple
	longName := "user-1-updated-with-a-longer-name-to-trigger-redirect"
	err = hp.UpdateRow(slot, []any{
		int64(1),
		longName,
		true,
	})
	require.NoError(t, err)

	// From HeapPage's perspective, reading by the same slot must still
	// return the latest values. The underlying Page is responsible for
	// redirect logic.
	row, err = hp.ReadRow(slot)
	require.NoError(t, err)
	require.Equal(t, int64(1), row[0].(int64))
	require.Equal(t, longName, row[1].(string))
	require.Equal(t, true, row[2].(bool))
}

func TestHeapPage_DeleteRow(t *testing.T) {
	hp := newTestHeapPage(t)

	values := []any{
		int64(1),
		"user-1",
		true,
	}
	slot, err := hp.InsertRow(values)
	require.NoError(t, err)

	// Delete the row
	err = hp.DeleteRow(slot)
	require.NoError(t, err)

	// Reading the deleted row should fail with storage.ErrBadSlot
	_, err = hp.ReadRow(slot)
	require.Error(t, err)
	require.True(t, errors.Is(err, storage.ErrBadSlot))
}
