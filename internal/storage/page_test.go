package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	defaultPageID = 0

	slot1Data = []byte("data string of slot 1")
	slot2Data = []byte("data string of slot 2")
	longData  = []byte("data string of slot longggggggggg" +
		" long longggggggggg long longggggggggg" +
		" long longggggggggg long longggggggggg" +
		" long longggggggggg long longggggggggg" +
		" long longggggggggg long longggggggggg" +
		" long longggggggggg long longggggggggg" +
		" long longggggggggg long longggggggggg" +
		" long longggggggggg long longggggggggg" +
		" long longggggggggg long longggggggggg" +
		" long longggggggggg long longggggggggg" +
		" long longggggggggg long longggggggggg" +
		" long longggggggggg long longggggggggg" +
		" long longggggggggg long longggggggggg",
	)
)

func newPage(t *testing.T) *Page {
	buf := make([]byte, PageSize)

	p, err := NewPage(buf, uint32(defaultPageID))
	require.NoError(t, err)

	// default after init page
	assert.Equal(t, uint16(PageSize), p.upper())
	assert.Equal(t, uint16(HeaderSize), p.lower())
	assert.Equal(t, 0, p.NumSlots())

	var slot int

	slot, err = p.InsertTuple(slot1Data)
	require.NoError(t, err)
	assert.Equal(t, 0, slot)

	slot, err = p.InsertTuple(slot2Data)
	require.NoError(t, err)
	assert.Equal(t, 1, slot)

	// after inserting two tuples
	assert.Equal(t, uint16(0x1fd6), p.upper())
	assert.Equal(t, uint16(0x18), p.lower())
	assert.Equal(t, 2, p.NumSlots())

	require.NotNil(t, p.DebugString())

	return p
}

func TestCRUDTuple(t *testing.T) {
	p := newPage(t)
	byteData, err := p.ReadTuple(0)
	require.NoError(t, err)
	assert.Equal(t, slot1Data, byteData)

	// bad slot
	_, err = p.ReadTuple(-1)
	require.ErrorIs(t, ErrBadSlot, err)
	_, err = p.ReadTuple(2)
	require.ErrorIs(t, ErrBadSlot, err)

	// deleted
	_ = p.DeleteTuple(0)
	_, err = p.ReadTuple(0)
	require.ErrorIs(t, ErrBadSlot, err)

	// moved -> update slot 1, it do not have efficient bytes len -> move to 2
	_ = p.UpdateTuple(1, longData)

	byteData, err = p.ReadTuple(2)
	require.NoError(t, err)
	byteData2, err := p.ReadTuple(1) // data same as slot 2 because it moved
	require.NoError(t, err)
	assert.Equal(t, byteData, byteData2)
}
