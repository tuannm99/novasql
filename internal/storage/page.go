package storage

const (
	_256   = 256
	_256_2 = 256 * 256
	_256_3 = 256 * 256 * 256
	_256_4 = 256 * 256 * 256 * 256
	_256_5 = _256_4 * 256
	_256_6 = _256_5 * 256
	_256_7 = _256_6 * 256
)

func GetU16(b []byte, offset int) uint16 {
	return uint16(b[offset]) + uint16(b[offset+1])*_256
}

func PutU16(b []byte, offset int, v uint16) {
	b[offset], b[offset+1] = byte(v%_256), byte(v/_256)
}

func GetU32(b []byte, offset int) uint32 {
	return uint32(b[offset]) +
		uint32(b[offset+1])*_256 +
		uint32(b[offset+2])*_256_2 +
		uint32(b[offset+3])*_256_3
}

func PutU32(b []byte, offset int, v uint32) {
	b[offset] = byte(v % _256)
	b[offset+1] = byte((v / _256) % _256)
	b[offset+2] = byte((v / (_256 * _256)) % _256)
	b[offset+3] = byte((v / (_256 * _256 * _256)) % _256)
}

// +------------------+ 0
// | PageHeaderData   |
// | LinePointers[]   | <-- pd_lower
// +------------------+
// |                  |
// |   Free space     |
// |                  |
// +------------------+ <-- pd_upper
// |  Tuple Data      |
// |  (grows down)    |
// +------------------+ <-- pd_special
// |  Special Space   |
// |  (fixed size)    |
// +------------------+ Block/Page Size (8192)
type Page struct {
	// buf := make([]byte, PageSize) -> max is only 8192
	Buf []byte
}

func NewPage(buf []byte, pageID uint32) Page {
	p := Page{Buf: buf}
	p.init(pageID)
	return p
}

// ---- Page methods ----
func (p Page) init(pageID uint32) {
	for i := range p.Buf {
		p.Buf[i] = 0
	}
	PutU16(p.Buf, 0, 0)          // flags
	PutU32(p.Buf, 2, pageID)     // page_id
	PutU16(p.Buf, 6, HeaderSize) // pd_lower
	PutU16(p.Buf, 8, PageSize)   // pd_upper
	PutU16(p.Buf, 10, PageSize)  // pd_special (unused yet)
}

func (p Page) Lower() int {
	return int(GetU16(p.Buf, 6))
}

func (p Page) SetLower(v int) {
	PutU16(p.Buf, 6, uint16(v))
}

func (p Page) Upper() int {
	return int(GetU16(p.Buf, 8))
}

func (p Page) SetUpper(v int) {
	PutU16(p.Buf, 8, uint16(v))
}

func (p Page) NumSlots() int {
	return (p.Lower() - HeaderSize) / SlotSize
}

func (p Page) slotOff(idx int) int {
	return HeaderSize + idx*SlotSize
}

func (p Page) GetSlot(i int) (offset, linePointer, flags int) {
	o := p.slotOff(i)
	return int(GetU16(p.Buf, o)),
		int(GetU16(p.Buf, o+2)),
		int(GetU16(p.Buf, o+4))
}

func (p Page) PutSlot(idx, offset, linePointer, flags int) {
	o := p.slotOff(idx)
	PutU16(p.Buf, o, uint16(offset))
	PutU16(p.Buf, o+2, uint16(linePointer))
	PutU16(p.Buf, o+4, uint16(flags))
}

func (p Page) appendSlot(offset, linePointer, flags int) int {
	i := p.NumSlots()
	p.PutSlot(i, offset, linePointer, flags)
	p.SetLower(p.Lower() + SlotSize)
	return i
}

func (p Page) IsUninitialized() bool {
	return GetU16(p.Buf, 6) == 0 && GetU16(p.Buf, 8) == 0
}

func (p Page) InsertTuple(tup []byte) (slot int, ok bool) {
	need := len(tup) + SlotSize
	if p.Upper()-p.Lower() < need {
		return -1, false
	}
	u := p.Upper() - len(tup)
	copy(p.Buf[u:], tup)
	p.SetUpper(u)
	return p.appendSlot(u, len(tup), 0), true
}

func (p Page) ReadTuple(slot int) ([]byte, bool) {
	if slot < 0 || slot >= p.NumSlots() {
		return nil, false
	}
	offset, linePointer, flags := p.GetSlot(slot)
	if flags != 0 || offset == 0 || linePointer == 0 {
		return nil, false
	}
	return p.Buf[offset : offset+linePointer], true
}

func (p Page) UpdateTuple(slot int, newTuple []byte) bool {
	offset, linePointer, flags := p.GetSlot(slot)
	if flags != 0 || offset == 0 || linePointer == 0 {
		return false
	}
	if len(newTuple) <= linePointer {
		copy(p.Buf[offset:], newTuple)
		p.PutSlot(slot, offset, len(newTuple), 0)
		return true
	}
	if _, ok := p.InsertTuple(newTuple); !ok {
		return false
	}
	p.PutSlot(slot, 0, 0, 2)
	return true
}

func (p Page) DeleteTuple(slot int) {
	p.PutSlot(slot, 0, 0, 1)
}
