package record

import (
	"testing"
	"time"

	"github.com/blbgo/general"

	"github.com/blbgo/record/store"
	"github.com/blbgo/testing/assert"
)

func TestOpenAndCloseDb(t *testing.T) {
	a := assert.New(t)

	st, err := store.New(store.NewConfigInMem())
	a.NoError(err)
	a.NotNil(st)

	db, err := New(st, []Record{&testRecord{}})
	a.NoError(err)
	a.NotNil(db)

	// create key without extra stuff
	keyField, err := BytesToTime(TimeToBytes(time.Now()))
	a.NoError(err)

	// write a new record
	tr := &testRecord{
		KeyField:  keyField,
		FirstName: "Brent",
		Age:       55,
	}
	a.NoError(db.Write(tr))

	// read it back
	tr2 := &testRecord{KeyField: keyField}
	a.NoError(db.Read(tr2))
	a.Equal(55, tr2.Age)

	// change it
	tr2.Age = 65
	a.NoError(db.Write(tr2))

	// read it back again
	a.NoError(db.Read(tr2))
	a.Equal(65, tr2.Age)

	// iterate
	tr2.KeyField = keyField.Add(-1 * time.Second)
	count := 0
	err = db.Range(tr2, 0, false, func(record Record) bool {
		tri := record.(*testRecord)
		a.Equal(keyField, tri.KeyField)
		a.Equal(65, tri.Age)
		count++
		return true
	})
	a.NoError(err)
	a.Equal(1, count)

	// delete it
	a.NoError(db.Delete(tr2))
	a.Error(db.Read(tr2))

	dc, ok := db.(general.DelayCloser)
	a.True(ok)
	doneChan := make(chan error)
	dc.Close(doneChan)
	a.NoError(<-doneChan)
}

func TestSequence(t *testing.T) {
	a := assert.New(t)

	st, err := store.New(store.NewConfigInMem())
	a.NoError(err)
	a.NotNil(st)

	db, err := New(st, []Record{&testRecord{}})
	a.NoError(err)
	a.NotNil(db)

	seq, err := db.GetSequence(&testRecord{Age: 55}, []byte("seq"))
	a.NoError(err)

	val, err := seq.Next()
	a.NoError(err)
	a.Log(val)

	dc, ok := db.(general.DelayCloser)
	a.True(ok)
	doneChan := make(chan error)
	dc.Close(doneChan)
	a.NoError(<-doneChan)
}
