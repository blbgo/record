package rootstate

import (
	"testing"

	"github.com/blbgo/general"
	"github.com/blbgo/record/root"
	"github.com/blbgo/record/store"
	"github.com/blbgo/testing/assert"
)

type testState struct {
	FirstName string
	LastName  string
	hidden    string
}

func TestCreate(t *testing.T) {
	a := assert.New(t)

	store, err := store.New(store.NewConfigInMem())
	a.NoError(err)
	a.NotNil(store)

	theRoot := root.New(store)
	a.NotNil(theRoot)

	thePersistentState, err := New(theRoot)
	a.NoError(err)
	a.NotNil(thePersistentState)

	aState := &testState{FirstName: "Brent", LastName: "Bergwall", hidden: "noSave"}
	err = thePersistentState.Save("stateName", aState)
	a.NoError(err)

	aState = &testState{}
	err = thePersistentState.Retrieve("stateName", aState)
	a.NoError(err)
	a.Equal("Brent", aState.FirstName)
	a.Equal("Bergwall", aState.LastName)
	a.Equal("", aState.hidden)

	c, ok := store.(general.DelayCloser)
	a.True(ok)
	doneChan := make(chan error, 100)
	c.Close(doneChan)
	a.Nil(<-doneChan)
}
