package root

import (
	"errors"

	badger "github.com/dgraph-io/badger/v2"
)

// ErrRangeSameOrBackwards range same key or backwards
var ErrRangeSameOrBackwards = errors.New("Range same key or backwards")

// ErrInvalidRootItemCall RootItem called on non root item
var ErrInvalidRootItemCall = errors.New("RootItem called on non root item")

// ErrDescriptionDoesNotMatch description does not match
var ErrDescriptionDoesNotMatch = errors.New("Description does not match")

// ErrBadRootKey root key not 2 bytes long
var ErrBadRootKey = errors.New("Root key not 2 bytes long")

// ErrInvalidIndex index number is out of range
var ErrInvalidIndex = errors.New("Index number is out of range")

// ErrNoFlags Bad value format, no flags byte
var ErrNoFlags = errors.New("Bad value format, no flags byte")

// ErrNoIndexCount Bad value format, no index count
var ErrNoIndexCount = errors.New("Bad value format, no index count")

// ErrNoIndexLength bad value format, no index length
var ErrNoIndexLength = errors.New("Bad value format, no index length")

// ErrBadIndexLength bad value format, index length more than value length
var ErrBadIndexLength = errors.New("Bad value format, index length more than value length")

// ErrCannotHaveChildren cannot have children
var ErrCannotHaveChildren = errors.New("Cannot have children")

// ErrCannotIndexChildren cannot index children
var ErrCannotIndexChildren = errors.New("Cannot index children")

// ErrCannotIndex cannot index
var ErrCannotIndex = errors.New("Cannot index")

// ErrChildrenHaveNoChildren indicates an attempt to specify canIndexChildren or
// childrenHaveChildren as true when there will be no children
var ErrChildrenHaveNoChildren = errors.New("Children have no children")

// ErrKeyInvalid key invalid length must greater than 2 and less than 256 bytes
var ErrKeyInvalid = errors.New("Key invalid length must greater than 2 and less than 256 bytes")

// ErrItemNotFound item not found
var ErrItemNotFound = badger.ErrKeyNotFound

// ErrEmptyIndex index is empty
var ErrEmptyIndex = errors.New("Index is empty")

// ErrIndexedItemNotFound failed to find a child by index
var ErrIndexedItemNotFound = errors.New("Indexed item not found")

// ErrTooManyIndexes More than 255 indexes specified
var ErrTooManyIndexes = errors.New("More than 255 indexes specified")

// ErrIndexTooLong a specified index is longer than 255 bytes
var ErrIndexTooLong = errors.New("A specified index is longer than 255 bytes")

// ErrAlreadyExists item with the specified key already exists
var ErrAlreadyExists = errors.New("Item with the specified key already exists")

// ErrIndexAlreadyExists index already exists for somthing else
var ErrIndexAlreadyExists = errors.New("Index already exists for somthing else")

// ErrPrefixCountToLong prefixCount to more than start key length
var ErrPrefixCountToLong = errors.New("prefixCount more than start key length")

// ErrChangeRoot root item may not be changed
var ErrChangeRoot = errors.New("Root item may not be changed")

const rootDepth = -1
const mainKeyPrefix = 0
const indexKeyPrefix = 1
const firstUserRootKey = 16

const metaIndexed = 1
