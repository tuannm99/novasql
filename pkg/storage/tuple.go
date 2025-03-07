package storage

import (
	"encoding/binary"
	"fmt"
)

type Tuple struct {
	ID   uint64
	Data []byte
}

// InsertTuple inserts a tuple into the page
func (p *Page) InsertTuple(tuple Tuple) error {
	offset := 4 // Start after the header

	// Find the first empty space
	for offset+8+len(tuple.Data) < len(p.Data) {
		if binary.LittleEndian.Uint64(p.Data[offset:offset+8]) == 0 { // Empty slot
			binary.LittleEndian.PutUint64(p.Data[offset:], tuple.ID)
			copy(p.Data[offset+8:], tuple.Data)
			p.dirty = true
			return nil
		}
		offset += 8 + len(tuple.Data)
	}

	return fmt.Errorf("page is full")
}

// FetchTuple retrieves a tuple by ID
func (p *Page) FetchTuple(tupleID uint64) (*Tuple, error) {
	offset := 4 // Start after the header

	for offset < len(p.Data) {
		id := binary.LittleEndian.Uint64(p.Data[offset : offset+8])
		if id == tupleID {
			return &Tuple{ID: id, Data: p.Data[offset+8:]}, nil
		}
		offset += 8 + len(p.Data[offset+8:])
	}

	return nil, fmt.Errorf("tuple not found")
}
