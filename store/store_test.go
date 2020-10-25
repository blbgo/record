package store

import (
	"io"
	"testing"

	badger "github.com/dgraph-io/badger/v2"

	"github.com/blbgo/testing/assert"
)

func TestOpenAndCloseDb(t *testing.T) {
	a := assert.New(t)

	dbFac, err := New(NewConfigInMem())
	a.NoError(err)
	a.NotNil(dbFac)

	db := dbFac.BadgerDB()
	a.NotNil(db)

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("abc"), []byte("xyz"))
	})
	a.NoError(err)

	var item *badger.Item = nil
	err = db.View(func(txn *badger.Txn) error {
		item, err = txn.Get([]byte("abc"))
		return err
	})
	a.NoError(err)
	a.NotNil(item)
	a.Equal("abc", string(item.Key()))
	var value string = ""
	item.Value(func(val []byte) error {
		value = string(val)
		return nil
	})
	a.Equal("xyz", value)

	c, ok := dbFac.(io.Closer)
	a.True(ok)
	c.Close()
}
