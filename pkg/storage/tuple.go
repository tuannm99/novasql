package storage

type TupleHeader struct {
	metadata interface{}
}

type Tuple struct {
	header TupleHeader
}

// example of schema
type TableData struct {
	ID          int64
	name        [255]byte
	description [1024]byte
}
