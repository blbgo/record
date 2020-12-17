// Package store provides the badger db to be used by higher level packages
package store

import (
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v2"
)

// Store allows writting, reading and searching records
type Store interface {
	BadgerDB() *badger.DB
	WriteBuffered(*badger.Entry)
	GetSequence(key []byte) (Sequence, error)
}

// Sequence provides a way to get ever incressing numbers
// *** needs to be moved somwhere more general
type Sequence interface {
	Next() (uint64, error)
}

type store struct {
	*badger.DB
	sequences []*badger.Sequence
	writeChan chan *badger.Entry
	doneChan  chan<- error
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
		DB:        db,
		writeChan: make(chan *badger.Entry, 100),
	}

	go newItem.background()

	return newItem, nil
}

func (r *store) Close(doneChan chan<- error) {
	r.doneChan = doneChan
	close(r.writeChan)
}

func (r *store) background() {
	timer := time.NewTimer(time.Minute)
	for {
		select {
		case entry, ok := <-r.writeChan:
			if !ok {
				r.close()
				return
			}
			var err error
			txn := r.DB.NewTransaction(true)
			done := false
			for !done {
				err = txn.SetEntry(entry)
				if err != nil {
					if err == badger.ErrTxnTooBig {
						txn.Commit()
						txn = r.DB.NewTransaction(true)
						continue
					}
					fmt.Println("record error writting in background")
					break
				}
				select {
				case entry, ok = <-r.writeChan:
					if !ok {
						done = true
					}
				default:
					done = true
				}
			}
			if err != nil {
				txn.Discard()
			} else {
				txn.Commit()
			}
			if !ok {
				r.close()
				return
			}
		case <-timer.C:
			err := r.RunValueLogGC(0.5)
			if err == nil {
				timer.Reset(5 * time.Minute)
			} else if err == badger.ErrNoRewrite {
				timer.Reset(time.Hour)
			} else {
				fmt.Println("badgerDB GC Unknown error:", err)
				timer.Reset(time.Hour)
			}
		}
	}
}

func (r *store) close() {
	for _, v := range r.sequences {
		v.Release()
	}
	r.doneChan <- r.DB.Close()
}

func (r *store) BadgerDB() *badger.DB {
	return r.DB
}

func (r *store) WriteBuffered(entry *badger.Entry) {
	r.writeChan <- entry
}

func (r *store) GetSequence(key []byte) (Sequence, error) {
	sequence, err := r.DB.GetSequence(key, 100)
	if err != nil {
		return nil, err
	}
	r.sequences = append(r.sequences, sequence)
	return sequence, nil
}
