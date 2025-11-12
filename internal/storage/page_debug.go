package storage

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"
)

// flag name pretty
func (p *Page) slotFlagName(f uint16) string {
	switch f {
	case SlotFlagNormal:
		return "NORMAL"
	case SlotFlagDeleted:
		return "DELETED"
	case SlotFlagMoved:
		return "MOVED"
	default:
		return fmt.Sprintf("UNKNOWN(0x%04x)", f)
	}
}

func utf8Preview(b []byte) string {
	if !utf8.Valid(b) {
		return ""
	}
	var buf bytes.Buffer
	for _, r := range string(b) { // duyệt theo rune
		if unicode.IsPrint(r) && r != '\n' && r != '\r' && r != '\t' {
			buf.WriteRune(r)
		} else {
			buf.WriteByte('.')
		}
	}
	return buf.String()
}

// ascii preview: printable -> itself, else '.'
func asciiPreview(b []byte) string {
	var buf bytes.Buffer
	for _, c := range b {
		r := rune(c)
		if unicode.IsPrint(r) && r != '\n' && r != '\r' && r != '\t' {
			buf.WriteRune(r)
		} else {
			buf.WriteByte('.')
		}
	}
	return buf.String()
}

// Debug prints header, line pointers, and tuple previews to writer.
func (p *Page) Debug(w io.Writer) {
	fmt.Fprintf(w, "=== Page Debug ===\n")
	fmt.Fprintf(w, "pageID=%d flags=0x%04x lower=%d upper=%d special=%d\n",
		p.PageID(), p.flags(), p.lower(), p.upper(), p.special())
	fmt.Fprintf(w, "pageSize=%d freeSpace=%d numSlots=%d\n",
		PageSize, p.FreeSpace(), p.NumSlots())

	// line pointers
	fmt.Fprintln(w, "\n-- LinePointers --")
	if p.NumSlots() == 0 {
		fmt.Fprintln(w, "(none)")
	}
	for i := 0; i < p.NumSlots(); i++ {
		s, err := p.getSlot(i)
		if err != nil {
			fmt.Fprintf(w, "[%d] <error: %v>\n", i, err)
			continue
		}
		fmt.Fprintf(w, "[%d] flags=%s off=%d len=%d\n",
			i, p.slotFlagName(s.Flags), s.Offset, s.Length)
	}

	// tuples preview
	fmt.Fprintln(w, "\n-- Tuples (preview) --")
	const maxPreview = 32
	if p.NumSlots() == 0 {
		fmt.Fprintln(w, "(none)")
	}
	for i := 0; i < p.NumSlots(); i++ {
		data, err := p.ReadTuple(i)
		if err != nil {
			// Deleted hoặc redirect-only sau khi follow sẽ rơi vào đây
			fmt.Fprintf(w, "[%d] (read) %v\n", i, err)
			continue
		}
		preview := data
		if len(preview) > maxPreview {
			preview = preview[:maxPreview]
		}
		hexDump := hex.EncodeToString(preview)
		fmt.Fprintf(w, "[%d] len=%d preview(hex)=%s\n", i, len(data), hexDump)
		fmt.Fprintf(w, "     preview(bytes)=%v\n", preview)

		if s := utf8Preview(preview); s != "" {
			fmt.Fprintf(w, "     preview(utf8)=\"%s\"\n", s)
		} else {
			fmt.Fprintf(w, "     preview(ascii)=\"%s\"\n", asciiPreview(preview))
		}
	}

	// free space window
	fmt.Fprintf(w, "\n-- FreeSpace --\nrange: [%d .. %d) size=%d bytes\n",
		p.lower(), p.upper(), p.FreeSpace())

	fmt.Fprintln(w, "=== End Page Debug ===")
}

func (p *Page) DebugString() string {
	var b bytes.Buffer
	p.Debug(&b)
	return b.String()
}
