package heap

import (
	"github.com/tuannm99/novasql/internal/record"
	"github.com/tuannm99/novasql/internal/storage"
)

// HeapPage = Page + Schema, Wrapper row-level on top of Page
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
