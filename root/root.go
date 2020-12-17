// Package root is a database system build on badgerDB
package root

import (
	"encoding/binary"

	badger "github.com/dgraph-io/badger/v2"

	"github.com/blbgo/record/store"
)

// Root provides access to depth 0 items
type Root interface {
	// RootItem create or find a depth 0 item
	// if the name already exists the description must match or an error is returned.  If it does
	// not exist it will be created with the provided description.
	RootItem(name, description string) (Item, error)
}

// New creates a Root
func New(store store.Store) Root {
	r := &item{
		Store: store,
		depth: rootDepth,
		value: []byte("root"),
	}

	return r
}

func (r *item) RootItem(name, description string) (Item, error) {
	if r.depth != rootDepth {
		return nil, ErrInvalidRootItemCall
	}

	item, err := r.ReadChildByIndex([]byte(name))
	if err == nil {
		if description != string(item.Value()) {
			return nil, ErrDescriptionDoesNotMatch
		}
		return item, nil
	}

	if err != badger.ErrKeyNotFound {
		return nil, err
	}

	err = nil
	var lastKey uint16 = firstUserRootKey - 1
	key := make([]byte, 2)
	binary.BigEndian.PutUint16(key, firstUserRootKey)
	// create root item
	r.RangeChildKeys(key, 0, false, func(key []byte) bool {
		if len(key) != 2 {
			err = ErrBadRootKey
			return false
		}
		intKey := binary.BigEndian.Uint16(key)
		if intKey <= lastKey {
			err = ErrRangeSameOrBackwards
			return false
		}
		if intKey > lastKey+1 {
			return false
		}
		lastKey = intKey
		return true
	})
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint16(key, lastKey+1)
	item, err = r.CreateChild(key, []byte(description), [][]byte{[]byte(name)})
	if err != nil {
		return nil, err
	}
	return item, nil
}
