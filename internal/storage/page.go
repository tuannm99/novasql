package storage

import (
	"errors"

	"github.com/tuannm99/novasql/pkg/bx"
)

// Fixed Header offsets
const (
	offFlags   = 0
	offPageID  = 2
	offLower   = 6
	offUpper   = 8
	offSpecial = 10
)

// tail (special)
const (
	offPageLSN = PageSize - 8
)

// Slot flags (similar to Postgres)
const (
	SlotFlagNormal  uint16 = 0      // LP_NORMAL
	SlotFlagDeleted uint16 = 1 << 0 // LP_DEAD
	SlotFlagMoved   uint16 = 1 << 1 // LP_REDIRECT
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
func (p *Page) flags() uint16       { return bx.U16At(p.Buf, offFlags) }
func (p *Page) setFlags(v uint16)   { bx.PutU16At(p.Buf, offFlags, v) }
func (p *Page) PageID() uint32      { return bx.U32At(p.Buf, offPageID) }
func (p *Page) setPageID(v uint32)  { bx.PutU32At(p.Buf, offPageID, v) }
func (p *Page) lower() uint16       { return bx.U16At(p.Buf, offLower) }
func (p *Page) setLower(v uint16)   { bx.PutU16At(p.Buf, offLower, v) }
func (p *Page) upper() uint16       { return bx.U16At(p.Buf, offUpper) }
func (p *Page) setUpper(v uint16)   { bx.PutU16At(p.Buf, offUpper, v) }
func (p *Page) special() uint16     { return bx.U16At(p.Buf, offSpecial) }
func (p *Page) setSpecial(v uint16) { bx.PutU16At(p.Buf, offSpecial, v) }

func (p *Page) PageLSN() uint64       { return bx.U64At(p.Buf, offPageLSN) }
func (p *Page) SetPageLSN(lsn uint64) { bx.PutU64At(p.Buf, offPageLSN, lsn) }

func (p *Page) IsUninitialized() bool { return p.lower() == 0 && p.upper() == 0 }
func (p *Page) FreeSpace() int        { return int(p.upper() - p.lower()) }
func (p *Page) NumSlots() int         { return int(p.lower()-HeaderSize) / SlotSize }
func (p *Page) slotOff(idx int) int   { return HeaderSize + idx*SlotSize }

func (p *Page) markRedirect(old, nw int) error {
	// Flags=MOVED, Offset=target slot, Length=0
	return p.putSlot(old, Slot{Offset: uint16(nw), Length: 0, Flags: SlotFlagMoved})
}

func (p *Page) init(pageID uint32) {
	for i := range p.Buf {
		p.Buf[i] = 0
	}
	p.setFlags(0)
	p.setPageID(pageID)
	p.setLower(HeaderSize)

	// reserve last 8 bytes for PageLSN
	special := uint16(PageSize - 8)
	p.setSpecial(special)
	p.setUpper(special)

	p.SetPageLSN(0)
}

// ---- slots ----
func (p *Page) getSlot(i int) (Slot, error) {
	if i < 0 || i >= p.NumSlots() {
		return Slot{}, ErrBadSlot
	}
	o := p.slotOff(i)
	// the slot must be within [HeaderSize, lower)
	if o+SlotSize > int(p.lower()) {
		return Slot{}, ErrCorruption
	}
	_ = p.Buf[o+5]
	return Slot{
		Offset: bx.U16(p.Buf[o+0:]),
		Length: bx.U16(p.Buf[o+2:]),
		Flags:  bx.U16(p.Buf[o+4:]),
	}, nil
}

func (p *Page) putSlot(idx int, s Slot) error {
	if idx < 0 || idx > p.NumSlots() {
		// allow writing the next slot only via append
		return ErrBadSlot
	}
	off := p.slotOff(idx)

	// if appending a new slot (idx == NumSlots), ensure there is space for the slot header
	if idx == p.NumSlots() && off+SlotSize > int(p.upper()) {
		return ErrNoSpace
	}
	// general bound protection
	if off+SlotSize > len(p.Buf) {
		return ErrCorruption
	}

	bx.PutU16(p.Buf[off+0:], s.Offset)
	bx.PutU16(p.Buf[off+2:], s.Length)
	bx.PutU16(p.Buf[off+4:], s.Flags)
	return nil
}

func (p *Page) appendSlot(off, length, flags uint16) (int, error) {
	i := p.NumSlots()
	newSlot := Slot{Offset: off, Length: length, Flags: flags}
	if err := p.putSlot(i, newSlot); err != nil {
		return -1, err
	}
	p.setLower(p.lower() + SlotSize)
	return i, nil
}

// IsLiveSlot reports whether the slot at the given index currently holds
// a visible (normal) tuple. Deleted or moved slots are treated as not live.
func (p *Page) IsLiveSlot(idx int) (bool, error) {
	s, err := p.getSlot(idx)
	if err != nil {
		// If slot index is out of range or corrupted, treat as not live
		// but bubble up serious errors if needed.
		if err == ErrBadSlot {
			return false, nil
		}
		return false, err
	}

	// Skip non-normal slots (deleted, moved, etc).
	if s.Flags != SlotFlagNormal {
		return false, nil
	}
	if s.Offset == 0 || s.Length == 0 {
		return false, nil
	}

	// Bounds checking for safety.
	start := int(s.Offset)
	end := start + int(s.Length)
	if start < 0 || end > PageSize || start >= end {
		return false, ErrCorruption
	}

	return true, nil
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

		if s.Flags == SlotFlagMoved {
			if s.Length != 0 || s.Offset == 0 {
				return nil, ErrCorruption
			}
			slot = int(s.Offset)
			visited++
			if visited > p.NumSlots() {
				return nil, ErrCorruption
			}
			continue
		}

		if s.Flags == SlotFlagDeleted {
			return nil, ErrBadSlot
		}
		if s.Flags != SlotFlagNormal {
			return nil, ErrCorruption
		}

		if s.Offset == 0 || s.Length == 0 {
			return nil, ErrCorruption
		}
		start, end := int(s.Offset), int(s.Offset)+int(s.Length)
		if start < 0 || start < int(p.upper()) || end > PageSize || start >= end {
			return nil, ErrCorruption
		}
		return p.Buf[start:end], nil
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

	// Need new space -> insert a fresh tuple, then redirect the old slot to the new slot
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

// Reset clears page content and re-initializes header.
// Useful for "rebuild page in-place" (e.g. BTree node rewrite).
func (p *Page) Reset(pageID uint32) {
	p.init(pageID)
}
