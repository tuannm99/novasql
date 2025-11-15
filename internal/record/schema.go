package record

type ColumnType uint8

const (
	ColInt32 ColumnType = iota
	ColInt64
	ColBool
	ColFloat64
	ColText  // UTF-8
	ColBytes // opaque bytes
)

type Column struct {
	Name     string
	Type     ColumnType
	Nullable bool
}

type Schema struct {
	Cols []Column
}

func (s Schema) NumCols() int { return len(s.Cols) }
