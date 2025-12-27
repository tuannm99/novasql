package clockx

// Clock implements CLOCK (second-chance) replacement for a fixed number of slots.
// It tracks ref bits and evictable state for slot IDs [0..capacity).
type Clock struct {
	ref       []bool
	evictable []bool
	present   []bool
	hand      int
	size      int // number of evictable slots
}

func New(capacity int) *Clock {
	if capacity <= 0 {
		capacity = 1
	}
	return &Clock{
		ref:       make([]bool, capacity),
		evictable: make([]bool, capacity),
		present:   make([]bool, capacity),
		hand:      0,
		size:      0,
	}
}

func (c *Clock) Capacity() int { return len(c.ref) }

// Touch marks slot as recently accessed.
func (c *Clock) Touch(id int) {
	if id < 0 || id >= len(c.ref) {
		return
	}
	if !c.present[id] {
		c.present[id] = true
	}
	c.ref[id] = true
}

// SetEvictable marks whether slot can be evicted (e.g., pin==0).
func (c *Clock) SetEvictable(id int, evictable bool) {
	if id < 0 || id >= len(c.ref) {
		return
	}
	if !c.present[id] {
		// Ignore unknown slot.
		return
	}

	old := c.evictable[id]
	if old == evictable {
		return
	}

	c.evictable[id] = evictable
	if evictable {
		c.size++
	} else {
		c.size--
	}
}

// Evict returns victim slot id and ok flag.
// It also removes the victim from tracking (present=false).
func (c *Clock) Evict() (id int, ok bool) {
	n := len(c.ref)
	if n == 0 || c.size == 0 {
		return -1, false
	}

	// Up to 2 sweeps to avoid infinite loops.
	for range 2 * n {
		idx := c.hand

		if c.present[idx] && c.evictable[idx] {
			if !c.ref[idx] {
				// Victim found -> remove it.
				c.present[idx] = false
				c.evictable[idx] = false
				c.ref[idx] = false
				c.size--

				c.hand = (c.hand + 1) % n
				return idx, true
			}
			// Second chance.
			c.ref[idx] = false
		}

		c.hand = (c.hand + 1) % n
	}

	return -1, false
}

// Remove removes slot from tracking (present=false).
func (c *Clock) Remove(id int) {
	if id < 0 || id >= len(c.ref) {
		return
	}
	if !c.present[id] {
		return
	}

	if c.evictable[id] {
		c.size--
	}
	c.present[id] = false
	c.evictable[id] = false
	c.ref[id] = false
}

func (c *Clock) Size() int { return c.size }
