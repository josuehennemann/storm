package msgpack

import (
	"testing"

	"github.com/josuehennemann/storm/codec/internal"
)

func TestMsgpack(t *testing.T) {
	internal.RoundtripTester(t, Codec)
}
