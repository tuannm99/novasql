package storage

import (
	"encoding/binary"
	"errors"
)

// Header offsets
const (
	offFlags   = 0
	offPageID  = 2
	offLower   = 6
	offUpper   = 8
	offSpecial = 10
)

// Slot flags
const (
	SlotFlagNormal  uint16 = 0
	SlotFlagDeleted uint16 = 1 << 0
	SlotFlagMoved   uint16 = 1 << 1
)

var (
	ErrTupleTooLarge = errors.New("page: tuple too large for inline")
	ErrNoSpace       = errors.New("page: not enough free space")
	ErrBadSlot       = errors.New("page: invalid slot")
	ErrCorruption    = errors.New("page: corrupt slot or tuple bounds")
	ErrWrongSize     = errors.New("page: buffer size != PageSize")
)

type Slot struct {
	Offset uint16
	Length uint16
	Flags  uint16
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
// +------------------+ <-- pd_special (unused)
// |  Special Space   |
// +------------------+ Block/Page Size (8192)
type Page struct {
	Buf []byte // fixed-size 8KB
}

func NewPage(buf []byte, pageID uint32) (*Page, error) {
	if len(buf) != PageSize {
		return nil, ErrWrongSize
	}
	p := &Page{Buf: buf}
	p.init(pageID)
	return p, nil
}

// ---- low-level header getters/setters ----
func (p *Page) flags() uint16 {
	return binary.LittleEndian.Uint16(p.Buf[offFlags:])
}

func (p *Page) setFlags(v uint16) {
	binary.LittleEndian.PutUint16(p.Buf[offFlags:], v)
}

func (p *Page) PageID() uint32 {
	return binary.LittleEndian.Uint32(p.Buf[offPageID:])
}

func (p *Page) setPageID(v uint32) {
	binary.LittleEndian.PutUint32(p.Buf[offPageID:], v)
}

func (p *Page) lower() uint16 {
	return binary.LittleEndian.Uint16(p.Buf[offLower:])
}

func (p *Page) setLower(v uint16) {
	binary.LittleEndian.PutUint16(p.Buf[offLower:], v)
}

func (p *Page) upper() uint16 {
	return binary.LittleEndian.Uint16(p.Buf[offUpper:])
}

func (p *Page) setUpper(v uint16) {
	binary.LittleEndian.PutUint16(p.Buf[offUpper:], v)
}

func (p *Page) special() uint16 {
	return binary.LittleEndian.Uint16(p.Buf[offSpecial:])
}

func (p *Page) setSpecial(v uint16) {
	binary.LittleEndian.PutUint16(p.Buf[offSpecial:], v)
}

func (p *Page) markRedirect(oldIdx, newIdx int) error {
	// Flags=Moved, Offset=slot đích, Length=0
	return p.putSlot(oldIdx, Slot{
		Offset: uint16(newIdx),
		Length: 0,
		Flags:  SlotFlagMoved,
	})
}

func (p *Page) init(pageID uint32) {
	// zero page
	for i := range p.Buf {
		p.Buf[i] = 0
	}
	p.setFlags(0)
	p.setPageID(pageID)
	p.setLower(HeaderSize)
	p.setUpper(PageSize)
	p.setSpecial(PageSize) // unused for now
}

// ---- public helpers ----
func (p *Page) FreeSpace() int {
	return int(p.upper() - p.lower())
}

func (p *Page) NumSlots() int {
	return int(p.lower()-HeaderSize) / SlotSize
}

func (p *Page) IsUninitialized() bool {
	return p.lower() == 0 && p.upper() == 0
}

// ---- slots ----
func (p *Page) slotOff(idx int) int {
	return HeaderSize + idx*SlotSize
}

func (p *Page) getSlot(i int) (Slot, error) {
	if i < 0 || i >= p.NumSlots() {
		return Slot{}, ErrBadSlot
	}
	o := p.slotOff(i)
	// slot phải nằm trong vùng [HeaderSize, lower)
	if o+SlotSize > int(p.lower()) {
		return Slot{}, ErrCorruption
	}
	_ = p.Buf[o+5]
	return Slot{
		Offset: binary.LittleEndian.Uint16(p.Buf[o+0:]),
		Length: binary.LittleEndian.Uint16(p.Buf[o+2:]),
		Flags:  binary.LittleEndian.Uint16(p.Buf[o+4:]),
	}, nil
}

func (p *Page) putSlot(idx int, s Slot) error {
	if idx < 0 || idx > p.NumSlots() {
		// allow writing next slot only via append
		return ErrBadSlot
	}
	off := p.slotOff(idx)

	// Nếu đang append slot mới (idx == NumSlots), đảm bảo còn chỗ cho header slot
	if idx == p.NumSlots() && off+SlotSize > int(p.upper()) {
		return ErrNoSpace
	}
	// Bảo vệ bounds chung
	if off+SlotSize > len(p.Buf) {
		return ErrCorruption
	}

	binary.LittleEndian.PutUint16(p.Buf[off+0:], s.Offset)
	binary.LittleEndian.PutUint16(p.Buf[off+2:], s.Length)
	binary.LittleEndian.PutUint16(p.Buf[off+4:], s.Flags)
	return nil
}

func (p *Page) appendSlot(off, length, flags uint16) (int, error) {
	i := p.NumSlots()
	if err := p.putSlot(i, Slot{Offset: off, Length: length, Flags: flags}); err != nil {
		return -1, err
	}
	p.setLower(p.lower() + SlotSize)
	return i, nil
}

// ---- tuples (payload) ----
func (p *Page) InsertTuple(tup []byte) (slot int, err error) {
	maxInline := PageSize - HeaderSize - SlotSize
	if len(tup) > maxInline {
		return -1, ErrTupleTooLarge
	}
	need := len(tup) + SlotSize
	if p.FreeSpace() < need {
		return -1, ErrNoSpace
	}
	u := int(p.upper()) - len(tup)
	copy(p.Buf[u:], tup)
	p.setUpper(uint16(u))
	return p.appendSlot(uint16(u), uint16(len(tup)), SlotFlagNormal)
}

func (p *Page) ReadTuple(slot int) ([]byte, error) {
	visited := 0
	for {
		s, err := p.getSlot(slot)
		if err != nil {
			return nil, err
		}

		switch s.Flags {
		case SlotFlagNormal:
			if s.Offset == 0 || s.Length == 0 {
				return nil, ErrCorruption
			}
			start, end := int(s.Offset), int(s.Offset)+int(s.Length)
			if start < 0 || start < int(p.upper()) || end > PageSize || start >= end {
				return nil, ErrCorruption
			}
			return p.Buf[start:end], nil

		case SlotFlagMoved:
			// follow redirect tới slot đích trong cùng page
			if s.Length != 0 || s.Offset == 0 {
				return nil, ErrCorruption
			}
			slot = int(s.Offset)
			visited++
			// tránh vòng lặp redirect bất thường
			if visited > p.NumSlots() {
				return nil, ErrCorruption
			}

		case SlotFlagDeleted:
			return nil, ErrBadSlot

		default:
			return nil, ErrCorruption
		}
	}
}

func (p *Page) UpdateTuple(slot int, newTuple []byte) error {
	s, err := p.getSlot(slot)
	if err != nil {
		return err
	}
	if s.Flags != SlotFlagNormal || s.Offset == 0 || s.Length == 0 {
		return ErrBadSlot
	}

	// In-place shrink or equal
	if len(newTuple) <= int(s.Length) {
		copy(p.Buf[int(s.Offset):], newTuple)
		return p.putSlot(slot, Slot{
			Offset: s.Offset,
			Length: uint16(len(newTuple)),
			Flags:  SlotFlagNormal,
		})
	}

	// Need new space -> insert, rồi redirect slot cũ sang slot mới
	newSlot, err := p.InsertTuple(newTuple)
	if err != nil {
		return err
	}
	return p.markRedirect(slot, newSlot)
}

func (p *Page) DeleteTuple(slot int) error {
	_, err := p.getSlot(slot)
	if err != nil {
		return err
	}
	return p.putSlot(slot, Slot{Offset: 0, Length: 0, Flags: SlotFlagDeleted})
}
