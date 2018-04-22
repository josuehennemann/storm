package engine

import (
	"encoding/binary"
	"errors"
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

func (b *FieldBuffer) Add(f *Field) {
	b.fields = append(b.fields, f)
}

func (b *FieldBuffer) Len() int {
	return len(b.fields)
}

type Schema struct {
	Fields map[string]*Field
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
	schema  *Schema
}

func NewRecordBuffer(s *Schema) *RecordBuffer {
	return &RecordBuffer{
		schema: s,
	}
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
	return b.schema, nil
}
