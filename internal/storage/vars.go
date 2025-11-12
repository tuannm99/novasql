package storage

import (
	"errors"
	"fmt"
)

const (
	OneB  = 1 << 0  // 1
	OneKB = 1 << 10 // 1,024
	OneMB = 1 << 20 // 1,048,576
	OneGB = 1 << 30 // 1,073,741,824

	SegmentSize       = 1 << 30                // 1,073,741,824 (1 GiB)
	PageSize          = 1 << 13                // 8,192 (8 KiB)
	MaxPagePerSegment = SegmentSize / PageSize // 131,072 pages/segment
	HeaderSize        = 12                     // 12
	SlotSize          = 6                      // 6 (3 * uint16: offset, length, flags)
)

const (
	FileMode0644 = 0o644
	FileMode0664 = 0o664
	FileMode0755 = 0o755
)

type PageType uint8

const (
	Slotted PageType = iota + 1
	BTree
	Overflow
)

type StorageMode int

const (
	Embedded   StorageMode = iota + 1 // sqlite
	Classic                           // postgres
	Document                          // mongodb
	WideColumn                        // cassandra
)

func (s StorageMode) String() string {
	switch s {
	case Embedded:
		return "embedded"
	case Classic:
		return "classic"
	case Document:
		return "document"
	case WideColumn:
		return "wide_column"
	default:
		return "unknown"
	}
}

func GetStorageMode(s string) (StorageMode, error) {
	switch s {
	case "embedded":
		return Embedded, nil
	case "classic":
		return Classic, nil
	case "document":
		return Document, nil
	case "wide_column":
		return WideColumn, nil
	default:
		return 0, fmt.Errorf("invalid storage mode: %s", s)
	}
}

var (
	ErrWriteExceedPageSize = errors.New("storage: write would exceed page size")
	ErrReadExceedPageSize  = errors.New("storage: read would exceed page size")
	ErrPageCorrupted       = errors.New("storage: page is corrupted")
	ErrBufferPoolFull      = errors.New("storage: buffer pool is full")
	ErrStorageIO           = errors.New("storage: I/O error")
	ErrInvalidOperation    = errors.New("storage: invalid operation")
	ErrNoFreeBuffer        = errors.New("storage: no free buffer available")
)
