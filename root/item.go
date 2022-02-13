package root

import (
	badger "github.com/dgraph-io/badger/v2"

	"github.com/blbgo/record/store"
)

// Item represents an item in the database
type Item interface {
	// CopyKey copies the items key into the provided buffer. If the buffer is too small a new
	// buffer will be allocated.
	CopyKey(buffer []byte) []byte
	// IndexCount returns the number of indexes the item has
	IndexCount() int
	// CopyIndex copies the specified index into the provided buffer. If the buffer is too small a
	// new buffer will be allocated.
	CopyIndex(index int, buffer []byte) ([]byte, error)
	// Value returns the value of the item
	Value() []byte
	// Update changes an item in the database
	Update(itemUpdate *ItemUpdate) error
	// UpdateValue changes only the value of an item, this is the same as calling Update with
	// IndexChanges and IndexAdditions empty
	UpdateValue(value []byte) error
	// DeleteChildren deletes all children of this item
	DeleteChildren() error
	// Delete deletes this item if it has no children, DeleteChildren should be called first if the
	// item has children. ErrMustDeleteChildrenFirst will be returned if there are any children
	Delete() error

	// Clone makes a full copy of an item, this is useful for saving an item for use outside of a
	// RangeChildren callback or other cases where the buffers inside an item are being reused.
	Clone() Item

	// CreateChild creates a child of this item
	CreateChild(key []byte, value []byte, indexes [][]byte) (Item, error)
	// QuickChild creates a new child with no indexes and does not return it
	QuickChild(key []byte, value []byte) error
	// ReadChild reads a child of this item
	ReadChild(key []byte) (Item, error)
	// ReadChildByIndex reads a child by one of it's indexes
	ReadChildByIndex(index []byte) (Item, error)
	// RangeChildren reads children calling cb for each one, the item provided to the callback is
	// only valid during the callback as it is reused for the next callback. If an item is needed
	// outsode the callback use Clone.
	RangeChildren(start []byte, prefixCount int, reverse bool, cb func(item Item) bool) error
	// RangeChildKeys reads the keys of children calling cb for each one
	RangeChildKeys(start []byte, prefixCount int, reverse bool, cb func(key []byte) bool) error
}

// ItemUpdate species what should be updated to the Item.Update method
type ItemUpdate struct {
	IndexChanges   []IndexChange
	IndexAdditions [][]byte
	Value          []byte
}

// IndexChange species an index to be changed in ItemUpdate
type IndexChange struct {
	Index    int
	NewIndex []byte
}

type item struct {
	store.Store
	depth   int
	fullKey []byte
	// baseKey and key will be slices of fullKey
	baseKey []byte
	key     []byte
	// value will be just the user value without flags or indexes
	indexes [][]byte
	value   []byte
}

func (r *item) CopyKey(buffer []byte) []byte {
	return append(buffer[:0], r.key...)
}

func (r *item) IndexCount() int {
	return len(r.indexes)
}

func (r *item) CopyIndex(index int, buffer []byte) ([]byte, error) {
	if index < 0 || index >= len(r.indexes) {
		return nil, ErrInvalidIndex
	}
	return append(buffer[:0], r.indexes[index]...), nil
}

func (r *item) Value() []byte {
	return r.value
}

func (r *item) Update(itemUpdate *ItemUpdate) error {
	if r.depth < 1 {
		return ErrChangeRoot
	}
	if len(r.indexes)+len(itemUpdate.IndexAdditions) > 255 {
		return ErrTooManyIndexes
	}
	for _, v := range itemUpdate.IndexAdditions {
		if len(v) > 255 {
			return ErrIndexTooLong
		}
	}
	for _, v := range itemUpdate.IndexChanges {
		if v.Index < 0 || v.Index >= len(r.indexes) {
			return ErrInvalidIndex
		}
		if len(v.NewIndex) > 255 {
			return ErrIndexTooLong
		}
	}
	saveIndexes := r.indexes
	r.indexes = append([][]byte{}, saveIndexes...)
	err := r.Store.BadgerDB().Update(func(txn *badger.Txn) error {
		for _, v := range itemUpdate.IndexChanges {
			newIndexKey := make([]byte, 0, len(r.baseKey)+1+len(v.NewIndex))
			newIndexKey = append(newIndexKey, r.baseKey...)
			newIndexKey = append(newIndexKey, indexKeyPrefix)
			newIndexKey = append(newIndexKey, v.NewIndex...)
			_, err := txn.Get(newIndexKey)
			if err != badger.ErrKeyNotFound {
				if err != nil {
					return err
				}
				return ErrIndexAlreadyExists
			}
			err = txn.Set(newIndexKey, r.key)
			if err != nil {
				return err
			}

			oldIndexKey := make([]byte, 0, len(r.baseKey)+1+len(r.indexes[v.Index]))
			oldIndexKey = append(oldIndexKey, r.baseKey...)
			oldIndexKey = append(oldIndexKey, indexKeyPrefix)
			oldIndexKey = append(oldIndexKey, r.indexes[v.Index]...)
			err = txn.Delete(oldIndexKey)
			if err != nil {
				return err
			}
			r.indexes[v.Index] = append([]byte{}, v.NewIndex...)
		}
		for _, v := range itemUpdate.IndexAdditions {
			newIndexKey := make([]byte, 0, len(r.baseKey)+1+len(v))
			newIndexKey = append(newIndexKey, r.baseKey...)
			newIndexKey = append(newIndexKey, indexKeyPrefix)
			newIndexKey = append(newIndexKey, v...)
			_, err := txn.Get(newIndexKey)
			if err != badger.ErrKeyNotFound {
				if err != nil {
					return err
				}
				return ErrIndexAlreadyExists
			}
			err = txn.Set(newIndexKey, r.key)
			if err != nil {
				return err
			}
			r.indexes = append(r.indexes, append([]byte{}, v...))
		}

		saveValue := r.value
		r.value = itemUpdate.Value
		newEntry := badger.NewEntry(r.fullKey, r.buildValue())
		if len(r.indexes) > 0 {
			newEntry.WithMeta(metaIndexed)
		}
		err := txn.SetEntry(newEntry)
		if err != nil {
			r.value = saveValue
			return err
		}
		return nil
	})
	if err != nil {
		r.indexes = saveIndexes
		return err
	}
	return nil
}

func (r *item) UpdateValue(value []byte) error {
	if r.depth < 1 {
		return ErrChangeRoot
	}
	oldValue := r.value
	r.value = value
	err := r.Store.BadgerDB().Update(func(txn *badger.Txn) error {
		newEntry := badger.NewEntry(r.fullKey, r.buildValue())
		if len(r.indexes) > 0 {
			newEntry.WithMeta(metaIndexed)
		}
		return txn.SetEntry(newEntry)
	})
	if err != nil {
		r.value = oldValue
		return err
	}
	return nil
}

func (r *item) DeleteChildren() error {
	if r.depth < 0 {
		return ErrChangeRoot
	}
	var err error
	r.RangeChildren(nil, 0, false, func(item Item) bool {
		err = item.DeleteChildren()
		if err != nil {
			return false
		}
		err = item.Delete()
		return err == nil
	})
	return err
}

func (r *item) Delete() error {
	err := r.DeleteChildren()
	if err != nil {
		return err
	}
	return r.Store.BadgerDB().Update(func(txn *badger.Txn) error {
		for _, v := range r.indexes {
			indexKey := make([]byte, 0, len(r.baseKey)+1+len(v))
			indexKey = append(indexKey, r.baseKey...)
			indexKey = append(indexKey, indexKeyPrefix)
			err := txn.Delete(append(indexKey, v...))
			if err != nil {
				return err
			}
		}
		return txn.Delete(r.fullKey)
	})
}

func (r *item) Clone() Item {
	clone := &item{
		Store:   r.Store,
		depth:   r.depth,
		fullKey: append([]byte{}, r.fullKey...),
		indexes: make([][]byte, 0, len(r.indexes)),
		value:   append([]byte{}, r.value...),
	}
	clone.baseKey = clone.fullKey[:len(r.baseKey)]
	clone.key = clone.fullKey[len(r.baseKey)+1:]
	for _, v := range r.indexes {
		r.indexes = append(r.indexes, append([]byte{}, v...))
	}
	return clone
}

func (r *item) CreateChild(key []byte, value []byte, indexes [][]byte) (Item, error) {
	lenKey := len(key)
	if lenKey < 2 || lenKey > 255 {
		return nil, ErrKeyInvalid
	}
	if len(indexes) > 255 {
		return nil, ErrTooManyIndexes
	}
	for _, v := range indexes {
		if len(v) > 255 {
			return nil, ErrIndexTooLong
		}
	}

	childItem := &item{
		Store: r.Store,
		depth: r.depth + 1,
		value: value,
	}

	fullKey := make([]byte, 0, len(r.fullKey)+1+lenKey)
	if r.depth >= 0 {
		fullKey = append(fullKey, r.baseKey...)
		fullKey = append(fullKey, byte(len(r.key)))
		fullKey = append(fullKey, r.key...)
	}
	childItem.baseKey = fullKey
	fullKey = append(fullKey, mainKeyPrefix)
	preKeyLen := len(fullKey)
	fullKey = append(fullKey, key...)
	childItem.key = fullKey[preKeyLen:]
	childItem.fullKey = fullKey
	for _, v := range indexes {
		childItem.indexes = append(childItem.indexes, append([]byte{}, v...))
	}
	err := r.Store.BadgerDB().Update(childItem.createItem)
	if err != nil {
		return nil, err
	}

	return childItem, nil
}

func (r *item) QuickChild(key []byte, value []byte) error {
	if r.depth < 0 {
		return ErrChangeRoot
	}
	lenKey := len(key)
	if lenKey < 2 || lenKey > 255 {
		return ErrKeyInvalid
	}

	fullKey := make([]byte, 0, len(r.fullKey)+1+lenKey)
	fullKey = append(fullKey, r.baseKey...)
	fullKey = append(fullKey, byte(len(r.key)))
	fullKey = append(fullKey, r.key...)
	fullKey = append(fullKey, mainKeyPrefix)
	fullKey = append(fullKey, key...)
	return r.Store.BadgerDB().Update(func(txn *badger.Txn) error {
		_, err := txn.Get(fullKey)
		if err != badger.ErrKeyNotFound {
			if err != nil {
				return err
			}
			return ErrAlreadyExists
		}
		return txn.Set(fullKey, value)
	})
}

func (r *item) ReadChild(key []byte) (Item, error) {
	lenKey := len(key)
	if lenKey < 2 || lenKey > 255 {
		return nil, ErrKeyInvalid
	}
	fullKey := make([]byte, 0, len(r.fullKey)+1+len(key))
	if r.depth >= 0 {
		fullKey = append(fullKey, r.baseKey...)
		fullKey = append(fullKey, byte(len(r.key)))
		fullKey = append(fullKey, r.key...)
	}
	fullKey = append(fullKey, mainKeyPrefix)
	preKeyLen := len(fullKey)
	fullKey = append(fullKey, key...)

	var childItem *item
	err := r.Store.BadgerDB().View(func(txn *badger.Txn) error {
		dbItem, err := txn.Get(fullKey)
		if err != nil {
			return err
		}
		childItem = &item{
			fullKey: fullKey,
			baseKey: fullKey[:preKeyLen-1],
			key:     fullKey[preKeyLen:],
		}
		return childItem.loadFromItem(dbItem)
	})
	if err != nil {
		return nil, err
	}

	childItem.Store = r.Store
	childItem.depth = r.depth + 1

	return childItem, nil
}

func (r *item) ReadChildByIndex(index []byte) (Item, error) {
	indexKey := make([]byte, 0, len(r.fullKey)+1+len(index))
	if r.depth >= 0 {
		indexKey = append(indexKey, r.baseKey...)
		indexKey = append(indexKey, byte(len(r.key)))
		indexKey = append(indexKey, r.key...)
	}
	baseKey := indexKey
	indexKey = append(indexKey, indexKeyPrefix)
	indexKey = append(indexKey, index...)

	var childItem *item
	err := r.Store.BadgerDB().View(func(txn *badger.Txn) error {
		dbItem, err := txn.Get(indexKey)
		if err != nil {
			return err
		}
		fullKey := append(baseKey, mainKeyPrefix)
		preKeyLen := len(fullKey)
		err = dbItem.Value(func(value []byte) error {
			fullKey = append(fullKey, value...)
			return nil
		})
		if err != nil {
			return err
		}
		dbItem, err = txn.Get(fullKey)
		if err == badger.ErrKeyNotFound {
			return ErrIndexedItemNotFound
		} else if err != nil {
			return err
		}
		childItem = &item{
			fullKey: fullKey,
			baseKey: fullKey[:len(baseKey)],
			key:     fullKey[preKeyLen:],
		}
		return childItem.loadFromItem(dbItem)
	})
	if err != nil {
		return nil, err
	}

	childItem.Store = r.Store
	childItem.depth = r.depth + 1

	return childItem, nil
}

func (r *item) RangeChildren(
	start []byte,
	prefixCount int,
	reverse bool,
	cb func(item Item) bool,
) error {
	if prefixCount > len(start) {
		return ErrPrefixCountToLong
	}
	fullPrefix := make([]byte, 0, len(r.fullKey)+1+len(start))
	if r.depth >= 0 {
		fullPrefix = append(fullPrefix, r.baseKey...)
		fullPrefix = append(fullPrefix, byte(len(r.key)))
		fullPrefix = append(fullPrefix, r.key...)
	}
	fullPrefix = append(fullPrefix, mainKeyPrefix)
	preKeyLen := len(fullPrefix)
	fullPrefix = append(fullPrefix, start[:prefixCount]...)
	fullStart := append(fullPrefix, start[prefixCount:]...)
	//prefix = append(r.key, prefix...)
	return r.Store.BadgerDB().View(func(txn *badger.Txn) error {
		itOps := badger.DefaultIteratorOptions
		itOps.Prefix = fullPrefix
		itOps.Reverse = reverse
		it := txn.NewIterator(itOps)
		defer it.Close()
		childItem := &item{
			Store: r.Store,
			depth: r.depth + 1,
		}
		for it.Seek(fullStart); it.Valid(); it.Next() {
			itemKey := it.Item().Key()
			childItem.fullKey = itemKey
			childItem.baseKey = itemKey[:preKeyLen-1]
			childItem.key = itemKey[preKeyLen:]
			err := childItem.loadFromItem(it.Item())
			if err != nil {
				return err
			}
			if !cb(childItem) {
				return nil
			}
		}
		return nil
	})
}

func (r *item) RangeChildKeys(
	start []byte,
	prefixCount int,
	reverse bool,
	cb func(key []byte) bool,
) error {
	if prefixCount > len(start) {
		return ErrPrefixCountToLong
	}
	fullPrefix := make([]byte, 0, len(r.fullKey)+1+len(start))
	if r.depth >= 0 {
		fullPrefix = append(fullPrefix, r.baseKey...)
		fullPrefix = append(fullPrefix, byte(len(r.key)))
		fullPrefix = append(fullPrefix, r.key...)
	}
	fullPrefix = append(fullPrefix, mainKeyPrefix)
	preKeyLen := len(fullPrefix)
	fullPrefix = append(fullPrefix, start[:prefixCount]...)
	fullStart := append(fullPrefix, start[prefixCount:]...)
	//prefix = append(r.key, prefix...)
	return r.Store.BadgerDB().View(func(txn *badger.Txn) error {
		itOps := badger.DefaultIteratorOptions
		itOps.Prefix = fullPrefix
		itOps.PrefetchValues = false
		itOps.Reverse = reverse
		it := txn.NewIterator(itOps)
		defer it.Close()
		for it.Seek(fullStart); it.Valid(); it.Next() {
			if !cb(it.Item().Key()[preKeyLen:]) {
				return nil
			}
		}
		return nil
	})
}

func (r *item) createItem(txn *badger.Txn) error {
	_, err := txn.Get(r.fullKey)
	if err != badger.ErrKeyNotFound {
		if err != nil {
			return err
		}
		return ErrAlreadyExists
	}
	for _, v := range r.indexes {
		indexKey := make([]byte, 0, len(r.baseKey)+1+len(v))
		indexKey = append(indexKey, r.baseKey...)
		indexKey = append(indexKey, indexKeyPrefix)
		indexKey = append(indexKey, v...)
		_, err = txn.Get(indexKey)
		if err != badger.ErrKeyNotFound {
			if err != nil {
				return err
			}
			return ErrIndexAlreadyExists
		}
		err = txn.Set(indexKey, r.key)
		if err != nil {
			return err
		}
	}

	newEntry := badger.NewEntry(r.fullKey, r.buildValue())
	if len(r.indexes) > 0 {
		newEntry.WithMeta(metaIndexed)
	}
	return txn.SetEntry(newEntry)
}

func (r *item) buildValue() []byte {
	if len(r.indexes) == 0 {
		return r.value
	}
	fullValueLen := 1
	for _, v := range r.indexes {
		fullValueLen += 1 + len(v)
	}
	fullValue := make([]byte, 0, fullValueLen+len(r.value))
	fullValue = append(fullValue, byte(len(r.indexes)))
	for _, v := range r.indexes {
		fullValue = append(fullValue, byte(len(v)))
		fullValue = append(fullValue, v...)
	}
	return append(fullValue, r.value...)
}

func (r *item) loadFromItem(item *badger.Item) error {
	return item.Value(func(value []byte) error {
		if item.UserMeta()&metaIndexed != 0 {
			if len(value) < 1 {
				return ErrNoIndexCount
			}
			r.indexes = r.indexes[:0]
			indexes := value[0]
			value = value[1:]
			for ; indexes > 0; indexes-- {
				if len(value) < 1 {
					return ErrNoIndexLength
				}
				indexLen := int(value[0])
				value = value[1:]
				if len(value) < indexLen {
					return ErrBadIndexLength
				}
				r.indexes = append(r.indexes, append([]byte{}, value[:indexLen]...))
				value = value[indexLen:]
			}
		}
		r.value = append(r.value[:0], value...)
		return nil
	})
}
