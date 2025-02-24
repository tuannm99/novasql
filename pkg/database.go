package pkg

import "github.com/tuannm99/novasql/pkg/storage"

type Constraint uint8

const (
	PrimaryIndex = iota + 1
	Unique
)

type (
	NovaSql struct {
		storageManager storage.StorageManager
		ctx            Context
		WorkDir        string
	}

	Context struct {
		tables  map[string]TableMeta
		maxSize uint32
	}

	TableMeta struct {
		root    uint32
		name    string
		schema  Schema
		indexes []IndexMetadata
		rowid   uint64
	}

	Schema struct {
		columns []Column
		index   map[string]uint32
	}

	Column struct {
		name        string
		dataTypes   interface{}
		constraints []Constraint
	}

	IndexMetadata struct {
		root   uint32
		name   string
		column Column
		schema Schema
		unique bool
	}
)
