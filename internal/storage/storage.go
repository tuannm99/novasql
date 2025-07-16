package storage

import (
	"fmt"

	"github.com/tuannm99/novasql/internal/storage/common"
	"github.com/tuannm99/novasql/internal/storage/embedded"
)

type StorageEngine interface {
	Init() error
	Close() error
	Read(key interface{}) ([]byte, error)
	Write(key interface{}, data []byte) error
	Delete(key interface{}) error
	Flush() error
}

func NewEngine(mode common.StorageMode, workdir string) (StorageEngine, error) {
	switch mode {
	case common.Embedded:
		pager, err := embedded.NewPager(workdir, common.PageSize)
		if err != nil {
			return nil, fmt.Errorf("error initilize pager %w", err)
		}
		return &embedded.EmbeddedEngine{Pager: pager}, nil

	// case common.Classic:
	// 	return &classic.ClassicEngine{ /*...*/ }, nil
	//
	// case common.Document:
	// 	return &document.DocumentEngine{ /*...*/ }, nil
	//
	// case common.WideColumn:
	// 	return &column.WideColumnEngine{ /*...*/ }, nil

	default:
		return nil, fmt.Errorf("unsupported storage mode")
	}
}
