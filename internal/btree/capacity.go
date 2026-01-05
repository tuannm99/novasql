package btree

import "github.com/tuannm99/novasql/internal/storage"

// maxEntriesPerPage returns the max number of fixed-size entries that can fit into
// a slotted page.
//
// Assumptions:
//   - Page layout is: [Header][SlotArray grows up][TupleData grows down]
//   - Each tuple consumes exactly 1 slot entry of size storage.SlotSize
//   - Each tuple payload is fixed length: entrySize
func maxEntriesPerPage(entrySize int) int {
	if entrySize <= 0 {
		return 0
	}
	free := storage.PageSize - storage.HeaderSize
	if free <= 0 {
		return 0
	}
	return free / (storage.SlotSize + entrySize)
}

func maxLeafEntriesPerPage() int {
	return maxEntriesPerPage(LeafEntrySize)
}

func maxInternalEntriesPerPage() int {
	return maxEntriesPerPage(InternalEntrySize)
}
