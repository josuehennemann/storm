package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type FieldType int

const (
	Int64Field FieldType = iota
	StringField
)

type Field struct {
	Name  string
	Type  FieldType
	Value interface{}
	Data  []byte
}

func (f *Field) Encode() ([]byte, error) {
	var err error

	if f.Data == nil {
		switch f.Type {
		case StringField:
			f.Data = []byte(f.Value.(string))
		case Int64Field:
			f.Data, err = EncodeInt64(f.Value.(int64))
		default:
			return nil, errors.New("unsupported type")
		}
	}

	return f.Data, err
}

func EncodeInt64(i int64) ([]byte, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, i)
	return buf[:n], nil
}

func DecodeInt64(v []byte) (int64, error) {
	i, n := binary.Varint(v)
	if n < 0 {
		return 0, errors.New("overflow")
	}

	return i, nil
}

type FieldBuffer struct {
	fields []*Field
	i      int
	Schema *Schema
}

func (b *FieldBuffer) Bytes(field string) ([]byte, error) {
	for _, f := range b.fields {
		if f.Name == field {
			return f.Encode()
		}
	}

	return nil, errors.New("field not found")
}

func (b *FieldBuffer) Next() (*Field, error) {
	if b.i >= len(b.fields) {
		return nil, nil
	}

	f := b.fields[b.i]
	b.i++

	return f, nil
}

func (b *FieldBuffer) Reset() {
	b.i = 0
	b.fields = b.fields[:0]
}

func (b *FieldBuffer) addField(name string, typ FieldType, val interface{}) error {
	if b.Schema == nil {
		b.Schema = new(Schema)
	}

	f := b.Schema.Get(name)
	if f == nil {
		b.Schema.Set(name, typ)
		f = b.Schema.Get(name)
	}

	if f.Type != typ {
		return errors.New("mismatched type")
	}

	fd, err := b.Schema.CreateField(name)
	if err != nil {
		return err
	}

	fd.Value = val
	b.fields = append(b.fields, fd)
	return nil
}

func (b *FieldBuffer) AddInt64(name string, i int64) error {
	return b.addField(name, Int64Field, i)
}

func (b *FieldBuffer) AddString(name string, s string) error {
	return b.addField(name, StringField, s)
}

func (b *FieldBuffer) Add(f *Field) {
	b.fields = append(b.fields, f)
}

func (b *FieldBuffer) Len() int {
	return len(b.fields)
}

type Schema struct {
	fields map[string]*Field
}

func (c *Schema) Get(name string) *Field {
	f, _ := c.fields[name]
	return f
}

func (c *Schema) Set(name string, t FieldType) {
	if c.fields == nil {
		c.fields = make(map[string]*Field)
	}

	c.fields[name] = &Field{Name: name, Type: t}
}

func (c *Schema) CreateField(name string) (*Field, error) {
	v, ok := c.fields[name]
	if !ok {
		return nil, fmt.Errorf("unknown field '%s'", name)
	}

	return &Field{
		Name: name,
		Type: v.Type,
	}, nil
}

type Record interface {
	Next() (*Field, error)
	Bytes(field string) ([]byte, error)
}

type RecordScanner struct {
	Record
}

func (r *RecordScanner) GetString(field string) (string, error) {
	v, err := r.Bytes(field)
	if err != nil {
		return "", err
	}

	return string(v), nil
}

func (r *RecordScanner) GetInt64(field string) (int64, error) {
	v, err := r.Bytes(field)
	if err != nil {
		return 0, err
	}

	return DecodeInt64(v)
}

type RecordBuffer struct {
	records []Record
	i       int
}

func (b *RecordBuffer) Add(rec Record) {
	b.records = append(b.records, rec)
}

func (b *RecordBuffer) Next() (Record, error) {
	if b.i < len(b.records) {
		r := b.records[b.i]
		b.i++
		return r, nil
	}

	return nil, nil
}

func (b *RecordBuffer) Schema() (*Schema, error) {
	if len(b.records) == 0 {
		return nil, errors.New("can't generate schema from empty record buffer")
	}

	r := b.records[0]
	var s Schema
	for {
		f, err := r.Next()
		if err != nil {
			return nil, err
		}

		s.Set(f.Name, f.Type)
	}

	return &s, nil
}
