package codec_test

import (
	"fmt"

	"github.com/josuehennemann/storm"
	"github.com/josuehennemann/storm/codec/gob"
	"github.com/josuehennemann/storm/codec/json"
	"github.com/josuehennemann/storm/codec/msgpack"
	"github.com/josuehennemann/storm/codec/protobuf"
	"github.com/josuehennemann/storm/codec/sereal"
)

func Example() {
	// The examples below show how to set up all the codecs shipped with Storm.
	// Proper error handling left out to make it simple.
	var gobDb, _ = storm.Open("gob.db", storm.Codec(gob.Codec))
	var jsonDb, _ = storm.Open("json.db", storm.Codec(json.Codec))
	var msgpackDb, _ = storm.Open("msgpack.db", storm.Codec(msgpack.Codec))
	var serealDb, _ = storm.Open("sereal.db", storm.Codec(sereal.Codec))
	var protobufDb, _ = storm.Open("protobuf.db", storm.Codec(protobuf.Codec))

	fmt.Printf("%T\n", gobDb.Codec())
	fmt.Printf("%T\n", jsonDb.Codec())
	fmt.Printf("%T\n", msgpackDb.Codec())
	fmt.Printf("%T\n", serealDb.Codec())
	fmt.Printf("%T\n", protobufDb.Codec())

	// Output:
	// *gob.gobCodec
	// *json.jsonCodec
	// *msgpack.msgpackCodec
	// *sereal.serealCodec
	// *protobuf.protobufCodec
}
