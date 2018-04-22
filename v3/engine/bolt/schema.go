package bolt

import (
	"bytes"
	"errors"

	"github.com/asdine/storm/v3/engine"
	bolt "github.com/coreos/bbolt"
)

const (
	schemaBucketName string = "__schema"
)

type schemaBucket struct {
	b *bolt.Bucket
}

func newSchemaBucket(b *bolt.Bucket) (*schemaBucket, error) {
	bs, err := b.CreateBucketIfNotExists([]byte(schemaBucketName))
	if err != nil {
		return nil, err
	}

	return &schemaBucket{
		b: bs,
	}, nil
}

func (s *schemaBucket) Set(f *engine.Field) error {
	name := []byte(f.Name)
	ft := []byte{byte(f.Type)}

	v := s.b.Get(name)
	if v == nil {
		return s.b.Put(name, ft)
	}

	if !bytes.Equal(v, ft) {
		return errors.New("type mismatch")
	}

	return nil
}

func (s *schemaBucket) Schema() (*engine.Schema, error) {
	schema := engine.Schema{
		Fields: make(map[string]*engine.Field),
	}

	err := s.b.ForEach(func(k, v []byte) error {
		if len(v) == 0 {
			return nil
		}

		name := string(k)
		schema.Fields[name] = &engine.Field{
			Name: name,
			Type: engine.FieldType(v[0]),
		}

		return nil
	})

	return &schema, err
}
