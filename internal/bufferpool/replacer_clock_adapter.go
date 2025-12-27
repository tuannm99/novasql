package bufferpool

import "github.com/tuannm99/novasql/pkg/clockx"

type clockAdapter struct {
	c *clockx.Clock
}

func newClockAdapter(capacity int) Replacer {
	return &clockAdapter{c: clockx.New(capacity)}
}

func (a *clockAdapter) RecordAccess(frameID int) {
	a.c.Touch(frameID)
}

func (a *clockAdapter) SetEvictable(frameID int, e bool) {
	a.c.SetEvictable(frameID, e)
}

func (a *clockAdapter) Evict() (int, bool) {
	return a.c.Evict()
}

func (a *clockAdapter) Remove(frameID int) {
	a.c.Remove(frameID)
}

func (a *clockAdapter) Size() int {
	return a.c.Size()
}
