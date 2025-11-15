package heap

import (
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/storage"
)

// Table là heap file logic: biết tên, schema, StorageManager, FileSet, PageCount.
type Table struct {
	Name      string
	Schema    record.Schema
	SM        *storage.StorageManager
	FS        storage.FileSet
	PageCount uint32 // hiện tại giữ ở memory; TODO: persist ra meta
}

// NewTable: tạo handle Table (không tạo file).
// pageCount ban đầu sẽ được engine truyền vào (VD đọc từ meta, hoặc 0 với table mới).
func NewTable(
	name string,
	schema record.Schema,
	sm *storage.StorageManager,
	fs storage.FileSet,
	pageCount uint32,
) *Table {
	return &Table{
		Name:      name,
		Schema:    schema,
		SM:        sm,
		FS:        fs,
		PageCount: pageCount,
	}
}

// Insert: V1 naive -> luôn ưu tiên page cuối, nếu đầy thì tạo page mới.
func (t *Table) Insert(values []any) (TID, error) {
	var pageID uint32
	if t.PageCount == 0 {
		pageID = 0
		t.PageCount = 1 // table mới, sẽ khởi tạo page 0 lần đầu LoadPage
	} else {
		pageID = t.PageCount - 1
	}

	for {
		p, err := t.SM.LoadPage(t.FS, pageID)
		if err != nil {
			return TID{}, err
		}
		hp := HeapPage{Page: p, Schema: t.Schema}

		slot, err := hp.InsertRow(values)
		if err == storage.ErrNoSpace {
			// Page full -> thử tạo page mới
			pageID = t.PageCount
			t.PageCount++
			continue
		}
		if err != nil {
			// Ví dụ ErrTupleTooLarge, ErrWrongSize, ...
			return TID{}, err
		}
		if err := t.SM.SavePage(t.FS, pageID, *p); err != nil {
			return TID{}, err
		}
		return TID{PageID: pageID, Slot: uint16(slot)}, nil
	}
}

// Get đọc một row theo TID (pageID, slot).
func (t *Table) Get(id TID) ([]any, error) {
	p, err := t.SM.LoadPage(t.FS, id.PageID)
	if err != nil {
		return nil, err
	}
	hp := HeapPage{Page: p, Schema: t.Schema}
	return hp.ReadRow(int(id.Slot))
}

// Scan duyệt toàn bộ row (simple V1).
// fn có thể là callback để user xử lý từng row.
func (t *Table) Scan(fn func(id TID, row []any) error) error {
	for pageID := uint32(0); pageID < t.PageCount; pageID++ {
		p, err := t.SM.LoadPage(t.FS, pageID)
		if err != nil {
			return err
		}
		hp := HeapPage{Page: p, Schema: t.Schema}

		// duyệt từng slot
		for slot := 0; slot < hp.Page.NumSlots(); slot++ {
			row, err := hp.ReadRow(slot)
			if err != nil {
				// Deleted / moved lỗi sẽ trả về ErrBadSlot/ErrCorruption – có thể chọn skip
				// nhưng để đơn giản, cứ trả lỗi luôn.
				return err
			}
			id := TID{PageID: pageID, Slot: uint16(slot)}
			if err := fn(id, row); err != nil {
				return err
			}
		}
	}
	return nil
}
