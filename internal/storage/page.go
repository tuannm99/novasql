package storage

import (
	"io"
	"os"
)

const PAGE_SIZE = 8192

type Page struct {
	Data   []byte
	PageID uint64
}

func NewPage(pageID uint64) *Page {
	return &Page{
		Data:   make([]byte, PAGE_SIZE),
		PageID: pageID,
	}
}

func (p *Page) ReadFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(int64(p.PageID*PAGE_SIZE), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = io.ReadFull(file, p.Data)
	return err
}

func (p *Page) WriteToFile(filePath string) error {
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(int64(p.PageID*PAGE_SIZE), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = file.Write(p.Data)
	return err
}
