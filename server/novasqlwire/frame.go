package novasqlwire

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

const (
	// MaxFrameSize limits memory usage on malformed/hostile input.
	MaxFrameSize = 8 << 20 // 8 MiB
)

// ReadFrame reads a single length-prefixed JSON frame.
func ReadFrame(r io.Reader, v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return fmt.Errorf("novasqlwire: empty frame")
	}
	if n > MaxFrameSize {
		return fmt.Errorf("novasqlwire: frame too large: %d > %d", n, MaxFrameSize)
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}

	if err := json.Unmarshal(buf, v); err != nil {
		return fmt.Errorf("novasqlwire: bad json: %w", err)
	}
	return nil
}

// WriteFrame writes v as a length-prefixed JSON frame.
func WriteFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("novasqlwire: marshal: %w", err)
	}
	if len(b) == 0 {
		return fmt.Errorf("novasqlwire: empty json")
	}
	if len(b) > MaxFrameSize {
		return fmt.Errorf("novasqlwire: json too large: %d > %d", len(b), MaxFrameSize)
	}

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
