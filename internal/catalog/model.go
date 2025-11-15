package catalog

import (
	"github.com/tuannm99/novasql/internal/record"
)

type TableMeta struct {
	Name      string          `json:"name"`
	FileBase  string          `json:"file_base"`
	PageCount uint32          `json:"page_count"`
	Columns   []record.Column `json:"columns"`
}
