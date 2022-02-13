package root

import (
	"testing"
	"time"

	"github.com/blbgo/general"
	"github.com/blbgo/record/store"
	"github.com/blbgo/testing/assert"
)

func TestCreate(t *testing.T) {
	a := assert.New(t)

	store, err := store.New(store.NewConfigInMem())
	a.NoError(err)
	a.NotNil(store)

	root := New(store)
	a.NotNil(root)

	testRoot, err := root.RootItem("testRoot", "A test root item")
	a.NoError(err)
	a.NotNil(testRoot)

	a.Equal(1, testRoot.IndexCount())
	a.Equal("A test root item", string(testRoot.Value()))

	// ******** QuickChild
	err = testRoot.QuickChild([]byte("quick child"), []byte("quick value"))
	a.NoError(err)

	item, err := testRoot.ReadChild([]byte("quick child"))
	a.NoError(err)
	a.NotNil(item)
	a.Equal(0, item.IndexCount())
	a.Equal("quick child", string(item.CopyKey(nil)))
	a.Equal("quick value", string(item.Value()))

	err = item.Delete()
	a.NoError(err)

	// ******** CreateChildExpiresAt
	err = testRoot.CreateChildExpiresAt(
		[]byte("expires child"),
		[]byte("expires value"),
		uint64(time.Now().Add(time.Second).Unix()),
	)
	a.NoError(err)

	item, err = testRoot.ReadChild([]byte("expires child"))
	a.NoError(err)
	a.NotNil(item)
	a.Equal(0, item.IndexCount())
	a.Equal("expires child", string(item.CopyKey(nil)))
	a.Equal("expires value", string(item.Value()))

	err = item.UpdateValue([]byte("expires value update"))
	a.Equal(ErrInvalidOnExpiring, err)

	time.Sleep(time.Second * 2)
	_, err = testRoot.ReadChild([]byte("expires child"))
	a.Equal(ErrItemNotFound, err)

	// ******** CreateChild
	item, err = testRoot.CreateChild(
		[]byte("test child"),
		[]byte("the value"),
		[][]byte{[]byte("test index")},
	)
	a.NoError(err)
	a.NotNil(item)

	item, err = testRoot.ReadChild([]byte("test child"))
	a.NoError(err)
	checkItem(a, item)

	item, err = testRoot.ReadChildByIndex([]byte("test index"))
	a.NoError(err)
	checkItem(a, item)

	calls := 0
	err = testRoot.RangeChildren(nil, 0, false, func(item Item) bool {
		calls++
		checkItem(a, item)
		return true
	})
	a.NoError(err)
	a.Equal(1, calls)

	err = item.QuickChild([]byte("child of child"), []byte("A child of the child of testRoot"))
	a.NoError(err)

	err = item.Delete()
	a.NoError(err)
	_, err = testRoot.ReadChildByIndex([]byte("test index"))
	a.Equal(ErrItemNotFound, err)

	c, ok := store.(general.DelayCloser)
	a.True(ok)
	doneChan := make(chan error, 100)
	c.Close(doneChan)
	a.Nil(<-doneChan)
}

func checkItem(a *assert.Assert, item Item) {
	a.NotNil(item)
	a.Equal(1, item.IndexCount())
	a.Equal("test child", string(item.CopyKey(nil)))
	indexValue, err := item.CopyIndex(0, nil)
	a.NoError(err)
	a.Equal("test index", string(indexValue))
	a.Equal("the value", string(item.Value()))
}
