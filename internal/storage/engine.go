package storage

import (
	"fmt"

	"github.com/tuannm99/novasql/internal/storage/common"
	"github.com/tuannm99/novasql/internal/storage/page"
)

func NewStorage(mode common.StorageMode, workdir string) (*page.Pager, error) {
	pager, err := page.NewPager(workdir, common.PageSize)
	if err != nil {
		return nil, fmt.Errorf("error initilize pager %w", err)
	}
	return pager, nil
}
