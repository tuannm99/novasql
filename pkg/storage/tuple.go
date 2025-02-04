package storage

type TupleHeader struct{}

type Tuple struct {
	header TupleHeader
}

type TableData struct {
	ID          int64
	name        [255]byte
	description [1024]byte
}
