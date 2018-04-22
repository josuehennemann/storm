package bolt

import (
	"bytes"
	"fmt"

	"github.com/asdine/storm/v3/engine"
	bolt "github.com/coreos/bbolt"
)

type bucket struct {
	buff engine.FieldBuffer
	b    *bolt.Bucket
	cur  *bolt.Cursor
	k, v []byte

	schema *engine.Schema
}

func newBucket(bb *bolt.Bucket) (*bucket, error) {
	sb, err := newSchemaBucket(bb)
	if err != nil {
		return nil, err
	}

	schema, err := sb.Schema()
	if err != nil {
		return nil, err
	}

	b := bucket{
		b:      bb,
		cur:    bb.Cursor(),
		schema: schema,
	}

	b.k, b.v = b.cur.First()
	return &b, nil
}

func (b *bucket) Next() (engine.Record, error) {
	if b.k == nil {
		return nil, nil
	}

	var id, fld, curid []byte

	b.buff.Reset()

	for b.k != nil {
		// skip buckets
		for b.k != nil && b.v == nil {
			b.k, b.v = b.cur.Next()
		}

		if b.k == nil {
			break
		}

		if idx := bytes.IndexByte(b.k, '-'); idx != -1 {
			id, fld = b.k[:idx], b.k[idx+1:]
		} else {
			return nil, fmt.Errorf("malformed rowid '%s'", b.k)
		}

		if curid != nil && !bytes.Equal(id, curid) {
			break
		}

		curid = id

		f := b.schema.Fields[string(fld)]
		b.buff.Add(&engine.Field{Name: f.Name, Type: f.Type, Data: b.v})

		b.k, b.v = b.cur.Next()
	}

	if b.buff.Len() == 0 {
		return nil, nil
	}

	return &b.buff, nil
}

func (b *bucket) Schema() (*engine.Schema, error) {
	return b.schema, nil
}
