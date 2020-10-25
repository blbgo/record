// Package store provides the badger db to be used by higher level packages
package store

import (
	badger "github.com/dgraph-io/badger/v2"
)

// Store allows writting, reading and searching records
type Store interface {
	BadgerDB() *badger.DB
}

type store struct {
	*badger.DB
}

// New creates a Store
func New(config Config) (Store, error) {
	dataPath := config.DataPath()
	options := badger.DefaultOptions(dataPath)
	options = options.WithLoggingLevel(badger.WARNING)
	options.Truncate = true // don't know what other option there is if data is corrupt?
	if dataPath == "" {
		options.InMemory = true
	}

	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}

	newItem := &store{
		DB: db,
	}

	return newItem, nil
}

func (r *store) Close() error {
	return r.DB.Close()
}

func (r *store) BadgerDB() *badger.DB {
	return r.DB
}
