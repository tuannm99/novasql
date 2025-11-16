package heap

import (
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/storage"
)

// HeapPage = Page + Schema, Wrapper for row-level action on top of Page,
// adding record decode/endcode each operation
// operation on row (values []any) instead of raw []byte.
type HeapPage struct {
	Page   *storage.Page
	Schema record.Schema
}

func NewHeapPage(p *storage.Page, s record.Schema) HeapPage {
	return HeapPage{Page: p, Schema: s}
}

func (hp *HeapPage) InsertRow(values []any) (int, error) {
	data, err := record.EncodeRow(hp.Schema, values)
	if err != nil {
		return -1, err
	}
	return hp.Page.InsertTuple(data)
}

func (hp *HeapPage) ReadRow(slot int) ([]any, error) {
	data, err := hp.Page.ReadTuple(slot)
	if err != nil {
		return nil, err
	}
	return record.DecodeRow(hp.Schema, data)
}

func (hp *HeapPage) UpdateRow(slot int, values []any) error {
	data, err := record.EncodeRow(hp.Schema, values)
	if err != nil {
		return err
	}
	return hp.Page.UpdateTuple(slot, data)
}

func (hp *HeapPage) DeleteRow(slot int) error {
	return hp.Page.DeleteTuple(slot)
}
