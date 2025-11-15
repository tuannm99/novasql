package catalog

import "github.com/tuannm99/novasql/internal/storage"

type TableMeta struct {
	Name      string           `json:"name"`
	FileBase  string           `json:"file_base"`
	PageCount uint32           `json:"page_count"`
	Columns   []storage.Column `json:"columns"`
}
