package record

import (
	"errors"
	"fmt"
	"math"

	"github.com/tuannm99/novasql/pkg/bx"
)

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

func (s Schema) Encode(values []any) ([]byte, error) { return EncodeRow(s, values) }
func (s Schema) Decode(buf []byte) ([]any, error)    { return DecodeRow(s, buf) }

// -----------------------------------------------------------------------------
// Row error kinds + sentinel errors
// -----------------------------------------------------------------------------

type RowErrKind string

const (
	RowErrValueCount     RowErrKind = "value_count"
	RowErrTypeMismatch   RowErrKind = "type_mismatch"
	RowErrNullNotAllowed RowErrKind = "null_not_allowed"
	RowErrVarTooLong     RowErrKind = "var_too_long"
	RowErrBufferShort    RowErrKind = "buffer_short"
	RowErrBadNullmap     RowErrKind = "bad_nullmap"
	RowErrUnsupported    RowErrKind = "unsupported_type"
)

// Sentinel errors dùng cho errors.Is.
var (
	ErrRowValueCount     = errors.New("record: value count mismatch")
	ErrRowTypeMismatch   = errors.New("record: type mismatch")
	ErrRowNullNotAllowed = errors.New("record: null not allowed")
	ErrRowVarTooLong     = errors.New("record: var too long")
	ErrRowBufferShort    = errors.New("record: buffer too short")
	ErrRowBadNullmap     = errors.New("record: bad nullmap")
	ErrRowUnsupported    = errors.New("record: unsupported type")
)

// RowError mang full context khi encode/decode fail.
type RowError struct {
	Op      string     // "encode" hoặc "decode"
	Kind    RowErrKind // loại lỗi
	ColIdx  int        // index cột, -1 nếu không gắn với cột
	ColName string     // tên cột (nếu có)
	Detail  string     // message chi tiết
	Base    error      // sentinel để errors.Is hoạt động
}

func (e *RowError) Error() string {
	if e.ColIdx >= 0 {
		if e.ColName != "" {
			return fmt.Sprintf("row %s error (%s) at col[%d:%s]: %s",
				e.Op, e.Kind, e.ColIdx, e.ColName, e.Detail)
		}
		return fmt.Sprintf("row %s error (%s) at col[%d]: %s",
			e.Op, e.Kind, e.ColIdx, e.Detail)
	}
	return fmt.Sprintf("row %s error (%s): %s", e.Op, e.Kind, e.Detail)
}

// Unwrap cho phép errors.Is / errors.As đi xuyên qua RowError tới Base.
func (e *RowError) Unwrap() error {
	return e.Base
}

func newRowError(
	op string,
	kind RowErrKind,
	colIdx int,
	colName string,
	base error,
	format string,
	args ...any,
) *RowError {
	return &RowError{
		Op:      op,
		Kind:    kind,
		ColIdx:  colIdx,
		ColName: colName,
		Base:    base,
		Detail:  fmt.Sprintf(format, args...),
	}
}

// -----------------------------------------------------------------------------
// EncodeRow(schema, values) -> []byte
//
// Format:
// [nullmap: ceil(N/8) bytes, bit=1 => NULL]  |  [field0 data?] [field1 data?] ...
// Varlen types (TEXT/BYTES): u16 length (LE) + data
// -----------------------------------------------------------------------------

func EncodeRow(s Schema, values []any) ([]byte, error) {
	nc := s.NumCols()
	if len(values) != nc {
		return nil, newRowError(
			"encode",
			RowErrValueCount,
			-1, "",
			ErrRowValueCount,
			"got %d values, want %d", len(values), nc,
		)
	}

	nbBytes := (nc + 7) / 8
	out := make([]byte, nbBytes)

	for i, col := range s.Cols {
		v := values[i]
		// NULL?
		if v == nil {
			if !col.Nullable {
				return nil, newRowError(
					"encode",
					RowErrNullNotAllowed,
					i, col.Name,
					ErrRowNullNotAllowed,
					"NULL not allowed for column",
				)
			}
			out[i/8] |= 1 << (uint(i) & 7)
			continue
		}

		switch col.Type {
		case ColInt32:
			x, ok := asInt32(v)
			if !ok {
				return nil, newRowError(
					"encode",
					RowErrTypeMismatch,
					i, col.Name,
					ErrRowTypeMismatch,
					"expected int32-compatible, got %T", v,
				)
			}
			var b [4]byte
			bx.PutU32(b[:], uint32(x))
			out = append(out, b[:]...)

		case ColInt64:
			x, ok := asInt64(v)
			if !ok {
				return nil, newRowError(
					"encode",
					RowErrTypeMismatch,
					i, col.Name,
					ErrRowTypeMismatch,
					"expected int64-compatible, got %T", v,
				)
			}
			var b [8]byte
			bx.PutU64(b[:], uint64(x))
			out = append(out, b[:]...)

		case ColBool:
			x, ok := v.(bool)
			if !ok {
				return nil, newRowError(
					"encode",
					RowErrTypeMismatch,
					i, col.Name,
					ErrRowTypeMismatch,
					"expected bool, got %T", v,
				)
			}
			if x {
				out = append(out, 1)
			} else {
				out = append(out, 0)
			}

		case ColFloat64:
			x, ok := asFloat64(v)
			if !ok {
				return nil, newRowError(
					"encode",
					RowErrTypeMismatch,
					i, col.Name,
					ErrRowTypeMismatch,
					"expected float64-compatible, got %T", v,
				)
			}
			var b [8]byte
			bx.PutU64(b[:], math.Float64bits(x))
			out = append(out, b[:]...)

		case ColText:
			str, ok := v.(string)
			if !ok {
				return nil, newRowError(
					"encode",
					RowErrTypeMismatch,
					i, col.Name,
					ErrRowTypeMismatch,
					"expected string, got %T", v,
				)
			}
			bs := []byte(str)
			if len(bs) > math.MaxUint16 {
				return nil, newRowError(
					"encode",
					RowErrVarTooLong,
					i, col.Name,
					ErrRowVarTooLong,
					"text length %d exceeds max %d", len(bs), math.MaxUint16,
				)
			}
			var l [2]byte
			bx.PutU16(l[:], uint16(len(bs)))
			out = append(out, l[:]...)
			out = append(out, bs...)

		case ColBytes:
			bs, ok := v.([]byte)
			if !ok {
				return nil, newRowError(
					"encode",
					RowErrTypeMismatch,
					i, col.Name,
					ErrRowTypeMismatch,
					"expected []byte, got %T", v,
				)
			}
			if len(bs) > math.MaxUint16 {
				return nil, newRowError(
					"encode",
					RowErrVarTooLong,
					i, col.Name,
					ErrRowVarTooLong,
					"bytes length %d exceeds max %d", len(bs), math.MaxUint16,
				)
			}
			var l [2]byte
			bx.PutU16(l[:], uint16(len(bs)))
			out = append(out, l[:]...)
			out = append(out, bs...)

		default:
			return nil, newRowError(
				"encode",
				RowErrUnsupported,
				i, col.Name,
				ErrRowUnsupported,
				"unsupported column type %v", col.Type,
			)
		}
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// DecodeRow(schema, buf) -> []any
// -----------------------------------------------------------------------------

func DecodeRow(s Schema, buf []byte) ([]any, error) {
	nc := s.NumCols()
	nbBytes := (nc + 7) / 8
	if len(buf) < nbBytes {
		return nil, newRowError(
			"decode",
			RowErrBadNullmap,
			-1, "",
			ErrRowBadNullmap,
			"buffer length %d < nullmap bytes %d", len(buf), nbBytes,
		)
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
				return nil, newRowError(
					"decode",
					RowErrBufferShort,
					colIdx, col.Name,
					ErrRowBufferShort,
					"need 4 bytes for int32, have %d", len(buf)-i,
				)
			}
			out[colIdx] = int32(bx.U32(buf[i : i+4]))
			i += 4

		case ColInt64:
			if i+8 > len(buf) {
				return nil, newRowError(
					"decode",
					RowErrBufferShort,
					colIdx, col.Name,
					ErrRowBufferShort,
					"need 8 bytes for int64, have %d", len(buf)-i,
				)
			}
			out[colIdx] = int64(bx.U64(buf[i : i+8]))
			i += 8

		case ColBool:
			if i+1 > len(buf) {
				return nil, newRowError(
					"decode",
					RowErrBufferShort,
					colIdx, col.Name,
					ErrRowBufferShort,
					"need 1 byte for bool, have %d", len(buf)-i,
				)
			}
			out[colIdx] = buf[i] != 0
			i++

		case ColFloat64:
			if i+8 > len(buf) {
				return nil, newRowError(
					"decode",
					RowErrBufferShort,
					colIdx, col.Name,
					ErrRowBufferShort,
					"need 8 bytes for float64, have %d", len(buf)-i,
				)
			}
			out[colIdx] = math.Float64frombits(bx.U64(buf[i : i+8]))
			i += 8

		case ColText:
			if i+2 > len(buf) {
				return nil, newRowError(
					"decode",
					RowErrBufferShort,
					colIdx, col.Name,
					ErrRowBufferShort,
					"need 2 bytes for text length, have %d", len(buf)-i,
				)
			}
			l := int(bx.U16(buf[i : i+2]))
			i += 2
			if i+l > len(buf) {
				return nil, newRowError(
					"decode",
					RowErrBufferShort,
					colIdx, col.Name,
					ErrRowBufferShort,
					"need %d bytes for text, have %d", l, len(buf)-i,
				)
			}
			out[colIdx] = string(buf[i : i+l])
			i += l

		case ColBytes:
			if i+2 > len(buf) {
				return nil, newRowError(
					"decode",
					RowErrBufferShort,
					colIdx, col.Name,
					ErrRowBufferShort,
					"need 2 bytes for bytes length, have %d", len(buf)-i,
				)
			}
			l := int(bx.U16(buf[i : i+2]))
			i += 2
			if i+l > len(buf) {
				return nil, newRowError(
					"decode",
					RowErrBufferShort,
					colIdx, col.Name,
					ErrRowBufferShort,
					"need %d bytes for bytes, have %d", l, len(buf)-i,
				)
			}
			cp := make([]byte, l)
			copy(cp, buf[i:i+l])
			out[colIdx] = cp
			i += l

		default:
			return nil, newRowError(
				"decode",
				RowErrUnsupported,
				colIdx, col.Name,
				ErrRowUnsupported,
				"unsupported column type %v", col.Type,
			)
		}
	}

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
