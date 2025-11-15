package heap

// TID (Tuple ID) row identity inside of heap file:
// PageID: page logic ID
// Slot  : slot index of page
type TID struct {
	PageID uint32
	Slot   uint16
}
