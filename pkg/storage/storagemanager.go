package storage

type BufferPool struct {
	pageDirectory PageDirectory
}

func (bp *BufferPool) getPage(pageID int) {
}

// mataining database files
type StorageManager struct{}
