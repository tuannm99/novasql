package common

import (
	"errors"
)

const (
	OneB  = 1
	OneKB = 1024
	OneMB = OneKB * 1024
	OneGB = OneMB * 1024

	PageSize = OneKB * 8
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

var (
	ErrPageNotFound        = errors.New("storage: page not found")
	ErrPageFull            = errors.New("storage: write would exceed page data length")
	ErrWriteExceedPageSize = errors.New("storage: write would exceed page size")
	ErrReadExceedPageSize  = errors.New("storage: read would exceed page size")
	ErrPageCorrupted       = errors.New("storage: page is corrupted")
	ErrBufferPoolFull      = errors.New("storage: buffer pool is full")
	ErrStorageIO           = errors.New("storage: I/O error")
	ErrInvalidOperation    = errors.New("storage: invalid operation")
	ErrNoFreeBuffer        = errors.New("storage: no free buffer available")
)
