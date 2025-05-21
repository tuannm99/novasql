package storage

import (
	"errors"
)

const (
	OneB  = 1
	OneKB = 1024
	OneMB = OneKB * 1024
	OneGB = OneMB * 1024
)

const (
	// 8KB page size, similar to PostgreSQL
	PageSize   = OneKB * 8
	CanCompact = 0x01
)

const (
	SegmentSize  = 1 * 1024 * 1024 * 1024
	FileMode0644 = 0o644 // rw-r--r--
	FileMode0664 = 0o664 // rw-rw-r--
	FileMode0755 = 0o755 // rwxr-xr-x
)

// PageType enum
type PageType uint8

const (
	Slotted PageType = iota + 1
	Directory
	BTree
	Overflow
)

// Common errors
var (
	ErrPageNotFound = errors.New("page not found")
	// ErrPageFull         = errors.New("page is full")
	ErrPageCorrupted    = errors.New("page is corrupted")
	ErrBufferPoolFull   = errors.New("buffer pool is full")
	ErrStorageIO        = errors.New("storage I/O error")
	ErrInvalidOperation = errors.New("invalid operation")
	ErrNoFreeBuffer     = errors.New("no free buffer available")
)
