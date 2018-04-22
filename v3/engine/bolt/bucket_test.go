package bolt

import (
	"fmt"
	"testing"

	"github.com/asdine/storm/v3/engine"
	"github.com/stretchr/testify/require"
)

func TestBucket(t *testing.T) {
	db, cleanup := tempDB(t)
	defer cleanup()

	e := NewEngine(db)
	tx, err := e.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	var buff engine.FieldBuffer
	for i := 0; i < 10; i++ {
		buff.Reset()

		buff.Add(&engine.Field{
			Name:  "Name",
			Type:  engine.StringField,
			Value: fmt.Sprintf("Name %d", i),
		})

		buff.Add(&engine.Field{
			Name:  "Age",
			Type:  engine.Int64Field,
			Value: int64(i),
		})

		_, err = tx.Insert(&buff, "a")
		require.NoError(t, err)
	}

	b, err := tx.Bucket("a")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		r, err := b.Next()
		require.NoError(t, err)
		require.NotNil(t, r)

		rs := engine.RecordScanner{Record: r}

		name, err := rs.GetString("Name")
		require.NoError(t, err)

		require.Equal(t, fmt.Sprintf("Name %d", i), name)

		age, err := rs.GetInt64("Age")
		require.NoError(t, err)
		require.Equal(t, int64(i), age)
	}

	r, err := b.Next()
	require.NoError(t, err)
	require.Nil(t, r)
}
