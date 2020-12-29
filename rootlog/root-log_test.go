package rootlog

import (
	"testing"
	"time"

	"github.com/blbgo/general"
	"github.com/blbgo/record/root"
	"github.com/blbgo/record/store"
	"github.com/blbgo/testing/assert"
)

func TestCreate(t *testing.T) {
	a := assert.New(t)

	store, err := store.New(store.NewConfigInMem())
	a.NoError(err)
	a.NotNil(store)

	theRoot := root.New(store)
	a.NotNil(theRoot)

	theRootLog, _, err := New(theRoot)
	a.NoError(err)
	a.NotNil(theRootLog)

	aLog, err := theRootLog.New("test log")
	a.NoError(err)

	err = aLog.Log("unformated message")
	a.NoError(err)

	err = aLog.Logf("formated message %v", 111)
	a.NoError(err)

	var aLogCreated time.Time
	count := 0
	theRootLog.Range(
		time.Now().Add(time.Hour*-1),
		false,
		func(created time.Time, name string) bool {
			a.Equal("test log", name)
			aLogCreated = created
			count++
			return true
		},
	)
	a.Equal(1, count)

	count = 0
	theRootLog.RangeLog(
		aLogCreated,
		time.Now().Add(time.Hour*-1),
		false,
		func(created time.Time, message string) bool {
			if count == 0 {
				a.Equal("unformated message", message)
			} else {
				a.Equal("formated message 111", message)
			}
			count++
			return true
		},
	)
	a.Equal(2, count)

	err = theRootLog.Delete(aLogCreated)
	a.NoError(err)

	_, err = theRootLog.Open(aLogCreated)
	a.Equal(root.ErrItemNotFound, err)

	c, ok := store.(general.DelayCloser)
	a.True(ok)
	doneChan := make(chan error, 100)
	c.Close(doneChan)
	a.Nil(<-doneChan)
}
