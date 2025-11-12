package main

import (
	"fmt"
	"log"
	"os"

	"github.com/tuannm99/novasql/internal/storage"
)

func WritePage(path string, p *storage.Page) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	off := int64(p.PageID()) * int64(storage.PageSize) // cần getter public nếu chưa có, tạm tính riêng
	// ví dụ: nếu chưa có p.PageID() public:
	// off := int64(0) // pageID=0

	if _, err := f.WriteAt(p.Buf, off); err != nil {
		return err
	}
	return f.Sync() // đảm bảo flush qua kernel buffers
}

func ReadPage(path string, pageID uint32) (*storage.Page, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, storage.PageSize)
	off := int64(pageID) * int64(storage.PageSize)
	if _, err := f.ReadAt(buf, off); err != nil {
		return nil, err
	}

	return storage.NewPage(buf, pageID) // sẽ re-init; nếu bạn muốn *giữ nguyên* header đọc từ đĩa,
	// hãy wrap bằng &storage.Page{Buf: buf} thay vì NewPage (NewPage sẽ gọi init).
}

func main() {
	// Tạo page rỗng, pageID=0
	buf := make([]byte, storage.PageSize)
	p, err := storage.NewPage(buf, 0)
	if err != nil {
		log.Fatal(err)
	}

	// Ghi xuống file
	if err := WritePage("data.rel", p); err != nil {
		log.Fatal(err)
	}
	fmt.Println(p)

	// Đọc lại và in 16 byte đầu dưới dạng hex
	f, _ := os.Open("data.rel")
	defer f.Close()
	h := make([]byte, 8192)
	if _, err := f.ReadAt(h, 0); err != nil {
		log.Fatal(err)
	}
	fmt.Println(h)
}
