package storage

import (
	"errors"
	"math"

	"github.com/tuannm99/novasql/internal/alias/bx"
)

// ---- Schema ----
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

// ---- Errors ----
var (
	ErrSchemaMismatch  = errors.New("rowcodec: schema/values mismatch")
	ErrBadBuffer       = errors.New("rowcodec: buffer underflow/overflow")
	ErrVarTooLong      = errors.New("rowcodec: variable length exceeds u16")
	ErrUnsupportedType = errors.New("rowcodec: unsupported type")
)

// ---- EncodeRow(schema, values) -> []byte ----
// Format:
// [nullmap: ceil(N/8) bytes, bit=1 => NULL]  |  [field0 data?] [field1 data?] ...
// Varlen types (TEXT/BYTES): u16 length (LE) + data
func EncodeRow(s Schema, values []any) ([]byte, error) {
	nc := s.NumCols()
	if len(values) != nc {
		return nil, ErrSchemaMismatch
	}

	// null bitmap
	nbBytes := (nc + 7) / 8
	out := make([]byte, nbBytes) // reserve nullmap first

	// encode fields
	for i, col := range s.Cols {
		v := values[i]
		// NULL?
		if v == nil {
			if !col.Nullable {
				return nil, ErrSchemaMismatch
			}
			out[i/8] |= 1 << (uint(i) & 7) // bit=1 => NULL
			continue
		}

		switch col.Type {
		case ColInt32:
			x, ok := asInt32(v)
			if !ok {
				return nil, ErrSchemaMismatch
			}
			var b [4]byte
			bx.PutU32(b[:], uint32(x))
			out = append(out, b[:]...)

		case ColInt64:
			x, ok := asInt64(v)
			if !ok {
				return nil, ErrSchemaMismatch
			}
			var b [8]byte
			bx.PutU64(b[:], uint64(x))
			out = append(out, b[:]...)

		case ColBool:
			x, ok := v.(bool)
			if !ok {
				return nil, ErrSchemaMismatch
			}
			if x {
				out = append(out, 1)
			} else {
				out = append(out, 0)
			}

		case ColFloat64:
			x, ok := asFloat64(v)
			if !ok {
				return nil, ErrSchemaMismatch
			}
			var b [8]byte
			bx.PutU64(b[:], math.Float64bits(x))
			out = append(out, b[:]...)

		case ColText:
			// expect string -> UTF-8 bytes
			str, ok := v.(string)
			if !ok {
				return nil, ErrSchemaMismatch
			}
			bs := []byte(str)
			if len(bs) > math.MaxUint16 {
				return nil, ErrVarTooLong
			}
			var l [2]byte
			bx.PutU16(l[:], uint16(len(bs)))
			out = append(out, l[:]...)
			out = append(out, bs...)

		case ColBytes:
			bs, ok := v.([]byte)
			if !ok {
				return nil, ErrSchemaMismatch
			}
			if len(bs) > math.MaxUint16 {
				return nil, ErrVarTooLong
			}
			var l [2]byte
			bx.PutU16(l[:], uint16(len(bs)))
			out = append(out, l[:]...)
			out = append(out, bs...)

		default:
			return nil, ErrUnsupportedType
		}
	}
	return out, nil
}

// ---- DecodeRow(schema, buf) -> []any ----
func DecodeRow(s Schema, buf []byte) ([]any, error) {
	nc := s.NumCols()
	nbBytes := (nc + 7) / 8
	if len(buf) < nbBytes {
		return nil, ErrBadBuffer
	}
	nullmap := buf[:nbBytes]
	i := nbBytes

	out := make([]any, nc)
	for colIdx, col := range s.Cols {
		isNull := (nullmap[colIdx/8]>>(uint(colIdx)&7))&1 == 1
		if isNull {
			out[colIdx] = nil
			continue
		}

		switch col.Type {
		case ColInt32:
			if i+4 > len(buf) {
				return nil, ErrBadBuffer
			}
			out[colIdx] = int32(bx.U32(buf[i : i+4]))
			i += 4

		case ColInt64:
			if i+8 > len(buf) {
				return nil, ErrBadBuffer
			}
			out[colIdx] = int64(bx.U64(buf[i : i+8]))
			i += 8

		case ColBool:
			if i+1 > len(buf) {
				return nil, ErrBadBuffer
			}
			out[colIdx] = buf[i] != 0
			i += 1

		case ColFloat64:
			if i+8 > len(buf) {
				return nil, ErrBadBuffer
			}
			out[colIdx] = math.Float64frombits(bx.U64(buf[i : i+8]))
			i += 8

		case ColText:
			if i+2 > len(buf) {
				return nil, ErrBadBuffer
			}
			l := int(bx.U16(buf[i : i+2]))
			i += 2
			if i+l > len(buf) {
				return nil, ErrBadBuffer
			}
			out[colIdx] = string(buf[i : i+l]) // UTF-8
			i += l

		case ColBytes:
			if i+2 > len(buf) {
				return nil, ErrBadBuffer
			}
			l := int(bx.U16(buf[i : i+2]))
			i += 2
			if i+l > len(buf) {
				return nil, ErrBadBuffer
			}
			// make a copy to avoid aliasing the page buffer
			cp := make([]byte, l)
			copy(cp, buf[i:i+l])
			out[colIdx] = cp
			i += l

		default:
			return nil, ErrUnsupportedType
		}
	}

	// (optional) i should == len(buf); nếu dư bytes là phần mở rộng tương lai
	return out, nil
}

// ---- small helpers to accept multiple numeric types on encode ----
func asInt32(v any) (int32, bool) {
	switch x := v.(type) {
	case int32:
		return x, true
	case int:
		if x >= math.MinInt32 && x <= math.MaxInt32 {
			return int32(x), true
		}
	case int64:
		if x >= math.MinInt32 && x <= math.MaxInt32 {
			return int32(x), true
		}
	}
	return 0, false
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	}
	return 0, false
}

func asFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	}
	return 0, false
}
