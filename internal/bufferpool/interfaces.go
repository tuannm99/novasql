package bufferpool

import "github.com/tuannm99/novasql/internal/storage"

type BufferPool interface {
	GetPage(fs storage.FileSet, pageID uint32) (*storage.Page, error)
	Unpin(page *storage.Page, dirty bool) error
	FlushAll() error
}
