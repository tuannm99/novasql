package bx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLittleEndianReadWrite verifies that PutU16/U32/U64 and U16/U32/U64
// correctly round-trip values using little-endian encoding.
func TestLittleEndianReadWrite(t *testing.T) {
	// ---- U16 ----
	{
		b := make([]byte, 2)
		var v uint16 = 0x1234 // 4660 decimal

		// write v -> b (little-endian)
		PutU16(b, v)

		// in LE, least-significant byte goes first
		assert.Equal(t, []byte{0x34, 0x12}, b)
		// read back
		assert.Equal(t, v, U16(b))
	}

	// ---- U32 ----
	{
		b := make([]byte, 4)
		var v uint32 = 0x01020304

		PutU32(b, v)
		// LE: 04 03 02 01
		assert.Equal(t, []byte{0x04, 0x03, 0x02, 0x01}, b)
		assert.Equal(t, v, U32(b))
	}

	// ---- U64 ----
	{
		b := make([]byte, 8)
		var v uint64 = 0x0102030405060708

		PutU64(b, v)
		// LE: 08 07 06 05 04 03 02 01
		assert.Equal(t, []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}, b)
		assert.Equal(t, v, U64(b))
	}
}

// TestLittleEndianAt verifies the *At variants that work with an offset
// into a larger buffer (common pattern when writing headers / slots).
func TestLittleEndianAt(t *testing.T) {
	buf := make([]byte, 16)

	// Write at different offsets
	PutU16At(buf, 0, 0x0A0B)
	PutU32At(buf, 2, 0x01020304)
	PutU64At(buf, 6, 0x0102030405060708)

	// U16 at offset 0
	assert.Equal(t, uint16(0x0A0B), U16At(buf, 0))

	// U32 at offset 2
	assert.Equal(t, uint32(0x01020304), U32At(buf, 2))

	// U64 at offset 6
	assert.Equal(t, uint64(0x0102030405060708), U64At(buf, 6))
}

// TestBigEndianReadWrite verifies BE helpers, primarily intended
// for sortable keys (index keys, range scans, etc.).
func TestBigEndianReadWrite(t *testing.T) {
	// ---- U16BE ----
	{
		b := make([]byte, 2)
		var v uint16 = 0x1234

		PutU16BE(b, v)
		// BE: most-significant byte first
		assert.Equal(t, []byte{0x12, 0x34}, b)
		assert.Equal(t, v, U16BE(b))
	}

	// ---- U32BE ----
	{
		b := make([]byte, 4)
		var v uint32 = 0x01020304

		PutU32BE(b, v)
		// BE: 01 02 03 04
		assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, b)
		assert.Equal(t, v, U32BE(b))
	}

	// ---- U64BE ----
	{
		b := make([]byte, 8)
		var v uint64 = 0x0102030405060708

		PutU64BE(b, v)
		// BE: 01 02 03 04 05 06 07 08
		assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, b)
		assert.Equal(t, v, U64BE(b))
	}
}

// TestBigEndianAt verifies the *BEAt variants with offsets.
func TestBigEndianAt(t *testing.T) {
	buf := make([]byte, 16)

	PutU16BEAt(buf, 0, 0x0A0B)
	PutU32BEAt(buf, 2, 0x01020304)
	PutU64BEAt(buf, 6, 0x0102030405060708)

	assert.Equal(t, uint16(0x0A0B), U16BEAt(buf, 0))
	assert.Equal(t, uint32(0x01020304), U32BEAt(buf, 2))
	assert.Equal(t, uint64(0x0102030405060708), U64BEAt(buf, 6))
}

// TestIntAliases checks I16/I32/I64 wrappers around U16/U32/U64.
func TestIntAliases(t *testing.T) {
	// int16
	{
		b := make([]byte, 2)
		var v int16 = -1234
		PutU16(b, uint16(v))
		assert.Equal(t, v, I16(b))
	}

	// int32
	{
		b := make([]byte, 4)
		var v int32 = -123456
		PutU32(b, uint32(v))
		assert.Equal(t, v, I32(b))
	}

	// int64
	{
		b := make([]byte, 8)
		var v int64 = -1234567890
		PutU64(b, uint64(v))
		assert.Equal(t, v, I64(b))
	}
}

// TestExampleFromComment is basically your original example,
// kept as a simple sanity check for U16 + PutU16.
func TestExampleFromComment(t *testing.T) {
	b := make([]byte, 2)
	sampleNum := uint16(14) // 0x000E

	PutU16(b, sampleNum)

	// LE: 0x0E, 0x00
	assert.Equal(t, []byte{0x0e, 0x00}, b)
	assert.Equal(t, sampleNum, U16(b))
}
