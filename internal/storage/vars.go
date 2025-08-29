package storage

import (
	"errors"
	"fmt"
)

const (
	OneB  = 1
	OneKB = 1024
	OneMB = OneKB * 1024
	OneGB = OneMB * 1024

	PageSize    = OneKB * 8
	SegmentSize = 1 << 30 // 1 GiB

	HeaderSize  = 24
	SlotSize    = 6
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
