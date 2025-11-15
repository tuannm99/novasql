package heap

import "github.com/tuannm99/novasql/internal/storage"

// HeapPage = Page + Schema, thao tác ở mức row (values []any) thay vì []byte.
type HeapPage struct {
	Pg     *storage.Page
	Schema storage.Schema
}

func NewHeapPage(p *storage.Page, s storage.Schema) HeapPage {
	return HeapPage{Pg: p, Schema: s}
}

func (hp *HeapPage) InsertRow(values []any) (int, error) {
	data, err := storage.EncodeRow(hp.Schema, values)
	if err != nil {
		return -1, err
	}
	return hp.Pg.InsertTuple(data)
}

func (hp *HeapPage) ReadRow(slot int) ([]any, error) {
	data, err := hp.Pg.ReadTuple(slot)
	if err != nil {
		return nil, err
	}
	return storage.DecodeRow(hp.Schema, data)
}
