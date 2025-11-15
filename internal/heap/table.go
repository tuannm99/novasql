package heap

import (
	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/storage"
)

// Table represent for heap file logic: name, schema, StorageManager, FileSet, PageCount.
type Table struct {
	Name      string
	Schema    record.Schema
	SM        *storage.StorageManager
	FS        storage.FileSet
	BP        bufferpool.Manager
	PageCount uint32 // hiện tại giữ ở memory; TODO: persist ra meta
}

// NewTable: tạo handle Table (không tạo file).
// pageCount ban đầu sẽ được engine truyền vào (VD đọc từ meta, hoặc 0 với table mới).
func NewTable(
	name string,
	schema record.Schema,
	sm *storage.StorageManager,
	fs storage.FileSet,
	bp bufferpool.Manager,
	pageCount uint32,
) *Table {
	return &Table{
		Name:      name,
		Schema:    schema,
		SM:        sm,
		FS:        fs,
		BP:        bp,
		PageCount: pageCount,
	}
}

// V1 naive -> always prefer last page, if page is full create new one
func (t *Table) Insert(values []any) (TID, error) {
	var pageID uint32
	if t.PageCount == 0 {
		pageID = 0
		t.PageCount = 1 // first page will be created lazily
	} else {
		pageID = t.PageCount - 1
	}

	for {
		// Use buffer pool instead of StorageManager directly
		p, err := t.BP.GetPage(pageID)
		if err != nil {
			return TID{}, err
		}

		hp := HeapPage{Page: p, Schema: t.Schema}

		slot, err := hp.InsertRow(values)
		if err == storage.ErrNoSpace {
			// Current page is full, unpin without dirty flag and try next page
			_ = t.BP.Unpin(p, false)

			pageID = t.PageCount
			t.PageCount++
			continue
		}
		if err != nil {
			// Unexpected error, unpin and return
			_ = t.BP.Unpin(p, false)
			return TID{}, err
		}

		// Mark page as dirty because we just inserted a tuple
		if err := t.BP.Unpin(p, true); err != nil {
			return TID{}, err
		}

		return TID{PageID: pageID, Slot: uint16(slot)}, nil
	}
}

// Get reads a single row by TID.
func (t *Table) Get(id TID) ([]any, error) {
	// Use buffer pool to get the page
	p, err := t.BP.GetPage(id.PageID)
	if err != nil {
		return nil, err
	}

	hp := HeapPage{Page: p, Schema: t.Schema}
	row, err := hp.ReadRow(int(id.Slot))

	// Read-only: dirty = false
	_ = t.BP.Unpin(p, false)

	if err != nil {
		return nil, err
	}
	return row, nil
}

// Scan iterates through all rows in the table.
func (t *Table) Scan(fn func(id TID, row []any) error) error {
	for pageID := uint32(0); pageID < t.PageCount; pageID++ {
		p, err := t.BP.GetPage(pageID)
		if err != nil {
			return err
		}

		hp := HeapPage{Page: p, Schema: t.Schema}

		// Iterate through all slots in the page
		for slot := 0; slot < hp.Page.NumSlots(); slot++ {
			row, err := hp.ReadRow(slot)
			if err != nil {
				// For simplicity we propagate the error;
				// later we may want to skip deleted/moved slots.
				_ = t.BP.Unpin(p, false)
				return err
			}
			id := TID{PageID: pageID, Slot: uint16(slot)}
			if err := fn(id, row); err != nil {
				_ = t.BP.Unpin(p, false)
				return err
			}
		}

		// Page was only read in Scan, not modified.
		_ = t.BP.Unpin(p, false)
	}
	return nil
}
