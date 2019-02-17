package json

import (
	"testing"

	"github.com/josuehennemann/storm/codec/internal"
)

func TestJSON(t *testing.T) {
	internal.RoundtripTester(t, Codec)
}
