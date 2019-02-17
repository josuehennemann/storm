package gob

import (
	"testing"

	"github.com/josuehennemann/storm/codec/internal"
)

func TestGob(t *testing.T) {
	internal.RoundtripTester(t, Codec)
}
