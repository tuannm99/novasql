package storage

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"
)

type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) Fprintf(format string, a ...any) {
	if e.err != nil {
		return
	}
	_, e.err = fmt.Fprintf(e.w, format, a...)
}

func (e *errWriter) Fprintln(a ...any) {
	if e.err != nil {
		return
	}
	_, e.err = fmt.Fprintln(e.w, a...)
}

// pretty flag names
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
	for _, r := range string(b) { // iterate by rune
		if unicode.IsPrint(r) && r != '\n' && r != '\r' && r != '\t' {
			buf.WriteRune(r)
		} else {
			buf.WriteByte('.')
		}
	}
	return buf.String()
}

// ASCII preview: printable -> itself, else '.'
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

// Debug prints header, line pointers, and tuple previews to the writer.
func (p *Page) Debug(w io.Writer) error {
	ew := &errWriter{w: w}

	ew.Fprintf("=== Page Debug ===\n")
	ew.Fprintf("pageID=%d flags=0x%04x lower=%d upper=%d special=%d\n",
		p.PageID(), p.flags(), p.lower(), p.upper(), p.special())
	ew.Fprintf("pageSize=%d freeSpace=%d numSlots=%d\n",
		PageSize, p.FreeSpace(), p.NumSlots())

	// line pointers
	ew.Fprintln("\n-- LinePointers --")
	if p.NumSlots() == 0 {
		ew.Fprintln("(none)")
	}
	for i := 0; i < p.NumSlots(); i++ {
		if ew.err != nil {
			break
		}
		s, err := p.getSlot(i)
		if err != nil {
			ew.Fprintf("[%d] <error: %v>\n", i, err)
			continue
		}
		ew.Fprintf("[%d] flags=%s off=%d len=%d\n",
			i, p.slotFlagName(s.Flags), s.Offset, s.Length)
	}

	// tuples preview
	ew.Fprintln("\n-- Tuples (preview) --")
	const maxPreview = 32
	if p.NumSlots() == 0 {
		ew.Fprintln("(none)")
	}
	for i := 0; i < p.NumSlots(); i++ {
		if ew.err != nil {
			break
		}
		data, err := p.ReadTuple(i)
		if err != nil {
			// Deleted or redirect-only (after following) will land here
			ew.Fprintf("[%d] (read) %v\n", i, err)
			continue
		}
		preview := data
		if len(preview) > maxPreview {
			preview = preview[:maxPreview]
		}
		hexDump := hex.EncodeToString(preview)
		ew.Fprintf("[%d] len=%d preview(hex)=%s\n", i, len(data), hexDump)
		ew.Fprintf("     preview(bytes)=%v\n", preview)

		if s := utf8Preview(preview); s != "" {
			ew.Fprintf("     preview(utf8)=\"%s\"\n", s)
		} else {
			ew.Fprintf("     preview(ascii)=\"%s\"\n", asciiPreview(preview))
		}
	}

	// free space window
	ew.Fprintf("\n-- FreeSpace --\nrange: [%d .. %d) size=%d bytes\n",
		p.lower(), p.upper(), p.FreeSpace())

	ew.Fprintln("=== End Page Debug ===")
	return ew.err
}

func (p *Page) DebugString() string {
	var b bytes.Buffer
	if err := p.Debug(&b); err != nil {
		// best-effort: surface the error in the output so callers see it
		_, _ = b.WriteString("\n<debug write error: " + err.Error() + ">\n")
	}
	return b.String()
}
