package storage

import (
	"context"
	"fmt"
	"os"

	"github.com/tuannm99/novasql/internal/storage/common"
	"github.com/tuannm99/novasql/internal/storage/page"
)

type BufferPool interface {
	FetchPage(pageID int) (*page.Page, error) // get from pool or load via Pager
	UnpinPage(pageID int, isDirty bool)       // mark dirty
	FlushPage(pageID int) error
	EvictPage() error
}

type PageOperation interface {
	Close(ctx context.Context) error
	Count(ctx context.Context) (int, error)
	Size(ctx context.Context) (int, error)
	Serialize(ctx context.Context) ([]byte, error)
	Deserialize(ctx context.Context, data []byte) error
}

type StorageOperation interface {
	NewStorage(filedir string, pageSize int) (PageOperation, error)
	GetPage(ctx context.Context, pageNum int) (PageOperation, error)
	WritePage(ctx context.Context, data []byte, pageNum int) error

	// files represent all the file loaded to memory, it contains split io, or just single file file[0]
	Files(ctx context.Context) []os.File
}

func NewStorage(mode common.StorageMode, workdir string) (*page.Pager, error) {
	pager, err := page.NewPager(workdir, common.PageSize)
	if err != nil {
		return nil, fmt.Errorf("error initilize storage %w", err)
	}
	return pager, nil
}
