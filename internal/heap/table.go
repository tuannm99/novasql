package heap

import (
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/tuannm99/novasql/internal/bufferpool"
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/storage"
	"github.com/tuannm99/novasql/pkg/bx"
)

// TID (Tuple ID) row identity inside of heap file:
// PageID: page logic ID
// Slot  : slot index of page
type TID struct {
	PageID uint32
	Slot   uint16
}

const (
	rowKindInline   = byte(0)
	rowKindOverflow = byte(1)
)

var ErrTableClosed = errors.New("heap: table is closed")

// Table represents heap file logic: name, schema, StorageManager, FileSet, PageCount.
type Table struct {
	Name      string
	Schema    record.Schema
	SM        *storage.StorageManager
	FS        storage.FileSet
	BP        bufferpool.Manager
	PageCount uint32

	// Overflow manager for large values of this table.
	Overflow *storage.OverflowManager

	// pageCountHook is a best-effort callback invoked when PageCount changes
	// (usually when allocating a new page).
	pageCountHook func(pageCount uint32) error

	closed atomic.Bool
}

func NewTable(
	name string,
	schema record.Schema,
	sm *storage.StorageManager,
	fs storage.FileSet,
	bp bufferpool.Manager,
	ovf *storage.OverflowManager,
	pageCount uint32,
) *Table {
	return &Table{
		Name:      name,
		Schema:    schema,
		SM:        sm,
		FS:        fs,
		BP:        bp,
		PageCount: pageCount,
		Overflow:  ovf,
	}
}

func (t *Table) SetPageCountHook(fn func(pageCount uint32) error) {
	t.pageCountHook = fn
}

// Insert inserts a new row into the heap.
func (t *Table) Insert(values []any) (TID, error) {
	if err := t.ensureOpen(); err != nil {
		return TID{}, err
	}

	oldPageCount := t.PageCount

	var pageID uint32
	if t.PageCount == 0 {
		pageID = 0
		t.PageCount = 1 // first page will be created lazily
	} else {
		pageID = t.PageCount - 1
	}

	tuple, err := t.encodeRowWithOverflow(values)
	if err != nil {
		return TID{}, err
	}

	for {
		p, err := t.BP.GetPage(pageID)
		if err != nil {
			return TID{}, err
		}

		slot, err := p.InsertTuple(tuple)
		if err == storage.ErrNoSpace {
			_ = t.BP.Unpin(p, false)

			pageID = t.PageCount
			t.PageCount++
			continue
		}
		if err != nil {
			_ = t.BP.Unpin(p, false)
			return TID{}, err
		}

		if err := t.BP.Unpin(p, true); err != nil {
			return TID{}, err
		}

		// Best-effort: if PageCount changed, sync meta.
		if t.PageCount != oldPageCount && t.pageCountHook != nil {
			if err := t.pageCountHook(t.PageCount); err != nil {
				slog.Warn("heap: pagecount hook failed", "table", t.Name, "pageCount", t.PageCount, "err", err)
			}
		}

		err = t.Flush()
		if err != nil {
			return TID{}, err
		}
		return TID{PageID: pageID, Slot: uint16(slot)}, nil
	}
}

// Get reads a single row by TID.
func (t *Table) Get(id TID) ([]any, error) {
	if err := t.ensureOpen(); err != nil {
		return nil, err
	}

	p, err := t.BP.GetPage(id.PageID)
	if err != nil {
		return nil, err
	}
	// Read-only access: always unpin with dirty=false
	defer func() { _ = t.BP.Unpin(p, false) }()

	raw, err := p.ReadTuple(int(id.Slot))
	if err != nil {
		return nil, err
	}
	return t.decodeRowWithOverflow(raw)
}

// Update updates a single row identified by TID.
func (t *Table) Update(id TID, values []any) error {
	if err := t.ensureOpen(); err != nil {
		return err
	}

	p, err := t.BP.GetPage(id.PageID)
	if err != nil {
		return err
	}

	dirty := false
	defer func() { _ = t.BP.Unpin(p, dirty) }()

	// 1) capture old overflow ref (if any)
	var oldRef *storage.OverflowRef
	oldRaw, err := p.ReadTuple(int(id.Slot))
	if err == nil && len(oldRaw) >= 1+8 && oldRaw[0] == rowKindOverflow {
		first := bx.U32(oldRaw[1:5])
		length := bx.U32(oldRaw[5:9])
		ref := storage.OverflowRef{FirstPageID: first, Length: length}
		oldRef = &ref
	}

	// 2) encode new tuple
	tuple, err := t.encodeRowWithOverflow(values)
	if err != nil {
		return err
	}

	// 3) update tuple
	if err := p.UpdateTuple(int(id.Slot), tuple); err != nil {
		return err
	}
	dirty = true

	// 4) free old overflow chain best-effort
	if oldRef != nil && t.Overflow != nil && oldRef.Length > 0 {
		if err := t.Overflow.Free(*oldRef); err != nil {
			slog.Warn("heap: overflow free failed after update (leak accepted)",
				"table", t.Name, "pageID", id.PageID, "slot", id.Slot,
				"first", oldRef.FirstPageID, "len", oldRef.Length, "err", err,
			)
		}
	}

	return t.Flush()
}

// Delete marks a single row identified by TID as deleted.
func (t *Table) Delete(id TID) error {
	if err := t.ensureOpen(); err != nil {
		return err
	}

	p, err := t.BP.GetPage(id.PageID)
	if err != nil {
		return err
	}

	dirty := false
	defer func() { _ = t.BP.Unpin(p, dirty) }()

	// capture overflow ref before delete
	var oldRef *storage.OverflowRef
	oldRaw, err := p.ReadTuple(int(id.Slot))
	if err == nil && len(oldRaw) >= 1+8 && oldRaw[0] == rowKindOverflow {
		first := bx.U32(oldRaw[1:5])
		length := bx.U32(oldRaw[5:9])
		ref := storage.OverflowRef{FirstPageID: first, Length: length}
		oldRef = &ref
	}

	if err := p.DeleteTuple(int(id.Slot)); err != nil {
		return err
	}
	dirty = true

	if oldRef != nil && t.Overflow != nil && oldRef.Length > 0 {
		if err := t.Overflow.Free(*oldRef); err != nil {
			slog.Warn("heap: overflow free failed after delete (leak accepted)",
				"table", t.Name, "pageID", id.PageID, "slot", id.Slot,
				"first", oldRef.FirstPageID, "len", oldRef.Length, "err", err,
			)
		}
	}

	return t.Flush()
}

// Scan iterates through all visible rows in the table.
// It skips deleted slots (ErrBadSlot) and returns other errors.
func (t *Table) Scan(fn func(id TID, row []any) error) error {
	if err := t.ensureOpen(); err != nil {
		return err
	}

	for pageID := uint32(0); pageID < t.PageCount; pageID++ {
		p, err := t.BP.GetPage(pageID)
		if err != nil {
			return err
		}

		for slot := 0; slot < p.NumSlots(); slot++ {
			raw, err := p.ReadTuple(slot)
			if errors.Is(err, storage.ErrBadSlot) {
				// Deleted tuple -> skip
				continue
			}
			if err != nil {
				_ = t.BP.Unpin(p, false)
				return err
			}

			row, err := t.decodeRowWithOverflow(raw)
			if err != nil {
				_ = t.BP.Unpin(p, false)
				return err
			}
			id := TID{PageID: pageID, Slot: uint16(slot)}

			if err := fn(id, row); err != nil {
				_ = t.BP.Unpin(p, false)
				return err
			}
		}

		_ = t.BP.Unpin(p, false)
	}
	return t.Flush()
}

func (t *Table) Flush() error {
	if err := t.BP.FlushAll(); err != nil {
		return err
	}

	if t.pageCountHook != nil {
		if err := t.pageCountHook(t.PageCount); err != nil {
			slog.Warn("heap: pagecount hook failed after flush", "table", t.Name, "pageCount", t.PageCount, "err", err)
		}
	}
	return nil
}

// encodeRowWithOverflow decides whether to store row inline or in overflow.
func (t *Table) encodeRowWithOverflow(values []any) ([]byte, error) {
	// 1) Encode full row like before.
	encoded, err := record.EncodeRow(t.Schema, values)
	if err != nil {
		return nil, err
	}

	// 2) If small enough for inline storage: prefix rowKindInline.
	// maxInline in Page.InsertTuple:
	//   maxInline := PageSize - HeaderSize - SlotSize
	// Here we need +1 for rowKind.
	maxInline := storage.PageSize - storage.HeaderSize - storage.SlotSize
	if len(encoded)+1 <= maxInline {
		out := make([]byte, 0, len(encoded)+1)
		out = append(out, rowKindInline)
		out = append(out, encoded...)
		return out, nil
	}

	// 3) Row is too large -> spill full encoded row to overflow.
	if t.Overflow == nil {
		return nil, fmt.Errorf("heap: overflow manager is nil for table %s", t.Name)
	}

	ref, err := t.Overflow.Write(encoded)
	if err != nil {
		return nil, err
	}

	// 4) On heap page we store pointer only: kind + ref (FirstPageID + Length).
	out := make([]byte, 0, 1+4+4)
	out = append(out, rowKindOverflow)

	var buf [4]byte
	// FirstPageID
	bx.PutU32(buf[:], ref.FirstPageID)
	out = append(out, buf[:]...)
	// Length
	bx.PutU32(buf[:], ref.Length)
	out = append(out, buf[:]...)

	return out, nil
}

// decodeRowWithOverflow decodes a tuple which may be inline or overflow-backed.
func (t *Table) decodeRowWithOverflow(raw []byte) ([]any, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("heap: empty tuple raw")
	}

	kind := raw[0]
	payload := raw[1:]

	switch kind {
	case rowKindInline:
		return record.DecodeRow(t.Schema, payload)

	case rowKindOverflow:
		if len(payload) < 8 {
			return nil, fmt.Errorf("heap: invalid overflow tuple size")
		}
		first := bx.U32(payload[0:4])
		length := bx.U32(payload[4:8])

		if t.Overflow == nil {
			return nil, fmt.Errorf("heap: overflow manager is nil for table %s", t.Name)
		}

		ref := storage.OverflowRef{
			FirstPageID: first,
			Length:      length,
		}
		full, err := t.Overflow.Read(ref)
		if err != nil {
			return nil, err
		}
		return record.DecodeRow(t.Schema, full)

	default:
		return nil, fmt.Errorf("heap: unknown row kind %d", kind)
	}
}

func (t *Table) Close() error {
	// idempotent
	if t == nil {
		return nil
	}
	if t.closed.Swap(true) {
		return nil
	}
	if t.BP != nil {
		return t.BP.FlushAll()
	}
	return nil
}

func (t *Table) ensureOpen() error {
	if t == nil {
		return ErrTableClosed
	}
	if t.closed.Load() {
		return ErrTableClosed
	}
	return nil
}
