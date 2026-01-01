package bufferpool

import "github.com/tuannm99/novasql/internal/storage"

// FileSetView binds a GlobalPool to a specific FileSet (relation).
// It implements Manager so heap/table/btree can use it without caring about FS.
type FileSetView struct {
	gp *GlobalPool
	fs storage.FileSet
}

func (v *FileSetView) GetPage(pageID uint32) (*storage.Page, error) {
	return v.gp.GetPage(v.fs, pageID)
}

func (v *FileSetView) Unpin(page *storage.Page, dirty bool) error {
	return v.gp.Unpin(v.fs, page, dirty)
}

// FlushAll flushes dirty pages for THIS FileSet only.
func (v *FileSetView) FlushAll() error {
	return v.gp.FlushFileSet(v.fs)
}

// View returns a relation-scoped Manager backed by the shared GlobalPool.
func (gp *GlobalPool) View(fs storage.FileSet) Manager {
	return &FileSetView{gp: gp, fs: fs}
}
