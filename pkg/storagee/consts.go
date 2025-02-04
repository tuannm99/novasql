package storage

const (
	OneB  = 1
	OneKB = 1024
	OneMB = OneKB * 1024
	OneGB = OneMB * 1024
)

const (
	PageSize   = OneKB * 8 // 8KB page size, similar to PostgreSQL
	CanCompact = 0x01
)

type PageType uint8

const (
	Root PageType = iota + 1
	Interior
	Leaf
)
