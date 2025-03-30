package storage

import (
	"encoding/binary"
	"fmt"
)

// Tuple represents a row in a table
type Tuple struct {
	ID   uint64
	Data []byte
}

// InsertTuple inserts a tuple into the page
func (p *Page) InsertTuple(tuple Tuple) error {
	if p.Header.Type != Slotted {
		return NewStorageError(ErrCodeInvalidOperation, "Cannot insert tuple into non-slotted page", nil)
	}

	// Serialize the tuple data
	tupleData := make([]byte, 8+len(tuple.Data))
	binary.LittleEndian.PutUint64(tupleData[:8], tuple.ID)
	copy(tupleData[8:], tuple.Data)

	// Add as a cell
	_, err := AddCell(p, tupleData)
	if err != nil {
		return err
	}

	p.dirty = true
	return nil
}

// FetchTuple retrieves a tuple by ID
func (p *Page) FetchTuple(tupleID uint64) (*Tuple, error) {
	if p.Header.Type != Slotted {
		return nil, NewStorageError(ErrCodeInvalidOperation, "Cannot fetch tuple from non-slotted page", nil)
	}

	// Get the pointer list
	pointerList, err := GetPointerList(p)
	if err != nil {
		return nil, err
	}

	// Search all cells for the tuple ID
	for i, ptr := range pointerList.Start {
		if ptr.Location == 0 || ptr.Size == 0 {
			continue // Skip deleted cells
		}

		// Get the cell data
		cellData, err := GetCell(p, uint32(i))
		if err != nil {
			continue
		}

		// Check if this is the tuple we're looking for
		if len(cellData) >= 8 {
			id := binary.LittleEndian.Uint64(cellData[:8])
			if id == tupleID {
				return &Tuple{
					ID:   id,
					Data: cellData[8:],
				}, nil
			}
		}
	}

	return nil, NewStorageError(ErrCodeInvalidOperation, fmt.Sprintf("Tuple with ID %d not found", tupleID), nil)
}
