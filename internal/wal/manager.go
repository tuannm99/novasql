package wal

import (
	"bufio"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/tuannm99/novasql/pkg/bx"
)

var (
	ErrBadMagic  = errors.New("wal: bad magic")
	ErrBadCRC    = errors.New("wal: bad crc")
	ErrBadRecord = errors.New("wal: bad record")
	ErrShortRead = errors.New("wal: short read")
	ErrNoWALFile = errors.New("wal: wal file not found")
)

const (
	magicU32   uint32 = 0x4C41574E // "NWAL"
	versionU16        = 1

	recPageImage uint8 = 1

	// Keep WAL independent from storage package.
	PageSize = 8192
)

// PageWriter allows WAL to apply redo without importing storage.
type PageWriter interface {
	WritePage(dir, base string, pageID uint32, pageBytes []byte) error
}

type Manager struct {
	mu      sync.Mutex
	f       *os.File
	path    string
	lsn     uint64
	flushed uint64
}

func Open(dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	m := &Manager{f: f, path: path}
	_ = m.initLastLSN()
	return m, nil
}

func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.f == nil {
		return nil
	}
	err := m.f.Close()
	m.f = nil
	return err
}

// AppendPageImage logs a full 8KB page image.
// NOTE: dir/base identify a relation file-set (LocalFileSet in storage).
func (m *Manager) AppendPageImage(dir, base string, pageID uint32, pageBytes []byte) (uint64, error) {
	if len(pageBytes) != PageSize {
		return 0, ErrBadRecord
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.f == nil {
		return 0, ErrNoWALFile
	}

	m.lsn++
	lsn := m.lsn

	dirB := []byte(filepath.Clean(dir))
	baseB := []byte(base)

	// fixed fields:
	// magic(4) ver(2) typ(1) rsv(1) totalLen(4) crc(4)
	// lsn(8) dirLen(2) baseLen(2) pageID(4)
	fixed := 4 + 2 + 1 + 1 + 4 + 4 + 8 + 2 + 2 + 4
	totalLen := fixed + len(dirB) + len(baseB) + PageSize

	buf := make([]byte, totalLen)
	off := 0

	putU32 := func(v uint32) { bx.PutU32(buf[off:off+4], v); off += 4 }
	putU16 := func(v uint16) { bx.PutU16(buf[off:off+2], v); off += 2 }
	putU64 := func(v uint64) { bx.PutU64(buf[off:off+8], v); off += 8 }
	putU8 := func(v uint8) { buf[off] = v; off++ }

	putU32(magicU32)
	putU16(versionU16)
	putU8(recPageImage)
	putU8(0)

	putU32(uint32(totalLen))

	crcOff := off
	putU32(0) // placeholder

	putU64(lsn)
	putU16(uint16(len(dirB)))
	putU16(uint16(len(baseB)))
	putU32(pageID)

	copy(buf[off:], dirB)
	off += len(dirB)
	copy(buf[off:], baseB)
	off += len(baseB)

	copy(buf[off:], pageBytes)
	off += PageSize

	if off != totalLen {
		return 0, ErrBadRecord
	}

	crc := crc32.ChecksumIEEE(buf[crcOff+4:])
	bx.PutU32(buf[crcOff:crcOff+4], crc)

	if _, err := m.f.Write(buf); err != nil {
		return 0, err
	}
	return lsn, nil
}

func (m *Manager) Flush(upto uint64) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.f == nil {
		return nil
	}
	if upto == 0 || upto <= m.flushed {
		return nil
	}
	if err := m.f.Sync(); err != nil {
		return err
	}
	m.flushed = upto
	return nil
}

// Recover replays WAL page images (redo) using writer.
func (m *Manager) Recover(writer PageWriter) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	path := m.path
	m.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer func() { _ = f.Close() }()

	r := bufio.NewReaderSize(f, 1<<20)

	for {
		rec, err := readOne(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			// tolerate torn tail record
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrShortRead) {
				return nil
			}
			return err
		}
		if rec.typ != recPageImage {
			continue
		}
		if err := writer.WritePage(rec.dir, rec.base, rec.pageID, rec.page); err != nil {
			return err
		}
	}
}

type decodedRecord struct {
	typ    uint8
	lsn    uint64
	dir    string
	base   string
	pageID uint32
	page   []byte
}

func readOne(r *bufio.Reader) (*decodedRecord, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	magic := bx.U32(hdr[:])
	if magic != magicU32 {
		return nil, ErrBadMagic
	}

	var verB [2]byte
	if _, err := io.ReadFull(r, verB[:]); err != nil {
		return nil, err
	}
	ver := bx.U16(verB[:])
	if ver != versionU16 {
		return nil, ErrBadRecord
	}

	tp, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if _, err := r.ReadByte(); err != nil { // reserved
		return nil, err
	}

	var lenB [4]byte
	if _, err := io.ReadFull(r, lenB[:]); err != nil {
		return nil, err
	}
	totalLen := bx.U32(lenB[:])
	if totalLen < uint32(4+2+1+1+4+4+8+2+2+4) {
		return nil, ErrBadRecord
	}

	var crcB [4]byte
	if _, err := io.ReadFull(r, crcB[:]); err != nil {
		return nil, err
	}
	wantCRC := bx.U32(crcB[:])

	restLen := int(totalLen) - (4 + 2 + 1 + 1 + 4 + 4)
	rest := make([]byte, restLen)
	if _, err := io.ReadFull(r, rest); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ErrShortRead
		}
		return nil, err
	}
	gotCRC := crc32.ChecksumIEEE(rest)
	if gotCRC != wantCRC {
		return nil, ErrBadCRC
	}

	off := 0
	getU64 := func() uint64 { v := bx.U64(rest[off : off+8]); off += 8; return v }
	getU16 := func() uint16 { v := bx.U16(rest[off : off+2]); off += 2; return v }
	getU32 := func() uint32 { v := bx.U32(rest[off : off+4]); off += 4; return v }

	lsn := getU64()
	dirLen := int(getU16())
	baseLen := int(getU16())
	pageID := getU32()

	if off+dirLen+baseLen+PageSize > len(rest) {
		return nil, ErrBadRecord
	}

	dir := string(rest[off : off+dirLen])
	off += dirLen
	base := string(rest[off : off+baseLen])
	off += baseLen

	page := make([]byte, PageSize)
	copy(page, rest[off:off+PageSize])

	return &decodedRecord{
		typ:    tp,
		lsn:    lsn,
		dir:    dir,
		base:   base,
		pageID: pageID,
		page:   page,
	}, nil
}

func (m *Manager) initLastLSN() error {
	f, err := os.Open(m.path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	r := bufio.NewReaderSize(f, 1<<20)
	var last uint64

	for {
		rec, err := readOne(r)
		if err != nil {
			break
		}
		if rec.lsn > last {
			last = rec.lsn
		}
	}

	if last > 0 {
		m.lsn = last
		m.flushed = last
	}
	return nil
}
