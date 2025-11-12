// TODO ----------------------
package novasql

import (
	"errors"
	"sync"
)

var (
	ErrDatabaseClosed = errors.New("novasql: database is closed")
	ErrInvalidPageID  = errors.New("novasql: invalid page ID")
)

type Database struct {
	mu     sync.RWMutex
	closed bool
}
