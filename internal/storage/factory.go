package storage

// import "github.com/tuannm99/novasql/internal/storage/embedded"

type PageOperation interface {
	Write(offset int, data []byte) error
	Read(offset, length int) (error, []byte)

	Serialize() (error, []byte)
	Deserialize(data []byte) error
}

type FileOperation interface {
	Load() (error, []byte) // load page to memory (pager,bufferpool,...)
	Flush() error          // flush page to disk
}

// type StorageFactory interface {
// 	GetPage(pageNum int) (PageOperation, error)
// 	WritePage(pageNum int, data []byte) error
// }
//
// func New(mode StorageMode, pageSize int) PageOperation {
// 	switch mode {
// 	case Embedded:
// 		return embedded.NewPager("default", pageSize)
// 	// case Classic:
// 	// 	return "classic"
// 	// case Document:
// 	// 	return "document"
// 	// case WideColumn:
// 	// 	return "wide_column"
// 	default:
// 		panic("wrong storage mode")
// 	}
// }
