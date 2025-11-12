// stand for bytes helper
package bx

import "encoding/binary"

var (
	LE = binary.LittleEndian
	BE = binary.BigEndian
)

// --- LE: read ---
func U16(b []byte) uint16 { return LE.Uint16(b) }
func U32(b []byte) uint32 { return LE.Uint32(b) }
func U64(b []byte) uint64 { return LE.Uint64(b) }
func I16(b []byte) int16  { return int16(U16(b)) }
func I32(b []byte) int32  { return int32(U32(b)) }
func I64(b []byte) int64  { return int64(U64(b)) }

// --- LE: write ---
func PutU16(b []byte, v uint16) { LE.PutUint16(b, v) }
func PutU32(b []byte, v uint32) { LE.PutUint32(b, v) }
func PutU64(b []byte, v uint64) { LE.PutUint64(b, v) }

// --- LE: At (offset) ---
func U16At(b []byte, off int) uint16       { return U16(b[off:]) }
func U32At(b []byte, off int) uint32       { return U32(b[off:]) }
func U64At(b []byte, off int) uint64       { return U64(b[off:]) }
func PutU16At(b []byte, off int, v uint16) { PutU16(b[off:], v) }
func PutU32At(b []byte, off int, v uint32) { PutU32(b[off:], v) }
func PutU64At(b []byte, off int, v uint64) { PutU64(b[off:], v) }

// --- BE (used for key/index sortable) ---
func U16BE(b []byte) uint16                  { return BE.Uint16(b) }
func U32BE(b []byte) uint32                  { return BE.Uint32(b) }
func U64BE(b []byte) uint64                  { return BE.Uint64(b) }
func PutU16BE(b []byte, v uint16)            { BE.PutUint16(b, v) }
func PutU32BE(b []byte, v uint32)            { BE.PutUint32(b, v) }
func PutU64BE(b []byte, v uint64)            { BE.PutUint64(b, v) }
func U16BEAt(b []byte, off int) uint16       { return U16BE(b[off:]) }
func U32BEAt(b []byte, off int) uint32       { return U32BE(b[off:]) }
func U64BEAt(b []byte, off int) uint64       { return U64BE(b[off:]) }
func PutU16BEAt(b []byte, off int, v uint16) { PutU16BE(b[off:], v) }
func PutU32BEAt(b []byte, off int, v uint32) { PutU32BE(b[off:], v) }
func PutU64BEAt(b []byte, off int, v uint64) { PutU64BE(b[off:], v) }
