package store

import (
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v2"

	"github.com/blbgo/general"
	"github.com/blbgo/testing/assert"
)

func TestOpenAndCloseDb(t *testing.T) {
	a := assert.New(t)

	store, err := New(NewConfigInMem())
	a.NoError(err)
	a.NotNil(store)

	db := store.BadgerDB()
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

	seq, err := store.GetSequence([]byte("seqtest"))
	num, err := seq.Next()
	a.NoError(err)
	a.Equal(uint64(0), num)

	store.WriteBuffered(badger.NewEntry([]byte("123"), []byte("456")))
	time.Sleep(2 * time.Second)
	err = db.View(func(txn *badger.Txn) error {
		item, err = txn.Get([]byte("123"))
		return err
	})
	a.NoError(err)
	a.NotNil(item)
	a.Equal("123", string(item.Key()))
	value = ""
	item.Value(func(val []byte) error {
		value = string(val)
		return nil
	})
	a.Equal("456", value)

	c, ok := store.(general.DelayCloser)
	a.True(ok)
	doneChan := make(chan error, 100)
	c.Close(doneChan)
	a.Nil(<-doneChan)
}
