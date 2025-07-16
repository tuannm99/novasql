package embedded

import "fmt"

type EmbeddedEngine struct {
	Pager *Pager
}

func (e *EmbeddedEngine) Init() error  { return nil }
func (e *EmbeddedEngine) Close() error { return e.Pager.Close() }

func (e *EmbeddedEngine) Read(key interface{}) ([]byte, error) {
	pageID, ok := key.(int)
	if !ok {
		return nil, fmt.Errorf("invalid key type for sqlite engine")
	}
	page, err := e.Pager.GetPage(pageID)
	if err != nil {
		return nil, err
	}
	return page.GetData(), nil
}

func (e *EmbeddedEngine) Write(key interface{}, data []byte) error {
	pageID, ok := key.(int)
	if !ok {
		return fmt.Errorf("invalid key type for sqlite engine")
	}
	return e.Pager.WritePage(pageID, data)
}

func (e *EmbeddedEngine) Delete(key interface{}) error {
	// No-op for SQLite style (unless you track free pages)
	return nil
}

func (e *EmbeddedEngine) Flush() error {
	// Optional: no-op or fsync
	return nil
}
