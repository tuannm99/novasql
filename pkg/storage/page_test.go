package storage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetPage(t *testing.T) {
	// assert equality
	assert.Equal(t, 123, 123, "they should be equal")

	// assert inequality
	assert.NotEqual(t, 123, 456, "they should not be equal")

}
