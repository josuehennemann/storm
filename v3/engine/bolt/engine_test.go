package bolt

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/asdine/storm/v3/engine"
	bolt "github.com/coreos/bbolt"
	"github.com/stretchr/testify/require"
)

func tempDB(t *testing.T) (*bolt.DB, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "stormv3")
	require.NoError(t, err)
	db, err := bolt.Open(path.Join(dir, "test.db"), 0660, nil)
	require.NoError(t, err)
	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}

func TestEngine(t *testing.T) {
	db, cleanup := tempDB(t)
	defer cleanup()

	e := NewEngine(db)
	tx, err := e.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	var buff engine.FieldBuffer

	buff.Add(&engine.Field{
		Name:  "Name",
		Type:  engine.StringField,
		Value: "Hello",
	})

	buff.Add(&engine.Field{
		Name:  "Age",
		Type:  engine.Int64Field,
		Value: int64(10),
	})

	_, err = tx.Insert(&buff, "a", "b")
	require.NoError(t, err)
}
