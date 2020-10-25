// Package record is a database system build on badgerDB
package record

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v2"

	"github.com/blbgo/record/store"
)

// Recorder allows writting, reading and searching records
type Recorder interface {
	// Write saves a record in the database, the provided records Key() and Record() are
	// what will be saved.  If a record with the same key already exists it will be
	// overwritten
	Write(record Record) error

	// Read retrives a record from the database using the key in the provided Record. The
	// provided records Record() will be overwritten with the retrived data. If the
	// requested record is not found ErrNotFound will be returned
	Read(record Record) error

	// Delete removes the record from the database with a key index matching the provided
	// records key.  record.Record() will not be used or updated.
	Delete(record Record) error

	// Range reads records starting at the key in the provided record until the call back function
	// returns false or the last record of the provided type is read
	Range(record Record, prefixBytes int, reverse bool, cb func(record Record) bool) error
}

// Sequence provides a way to get ever incressing numbers
type Sequence interface {
	Next() (uint64, error)
}

// RecorderDB is an interface to a base database
type RecorderDB interface {
	Recorder

	// WriteBuffered works like Write except the record is queued to be written by a background
	// worked thread.  This should allow several writes to be done under the same transaction for
	// better efficiency.  Should only be used for non-critical writes like log messages.
	WriteBuffered(record Record) error

	DeletePrefix(record Record, keyPrefix []byte) error

	GetSequence(record Record, key []byte) (Sequence, error)

	NewTransaction(update bool) RecorderTxn
}

// Record represents an individual record as well as the record type
type Record interface {
	// Name must return the three character code that represents this record type. This must be a
	// constant for a specific implementation and must be unique across all record types provided
	// by Config.Records. This code must consist of all lowercase letters and be exactly 3
	// characters long
	Name() string

	// Index must return a byte slice representation of the indicated index, ErrInvalidIndex, or
	// some other error indicating the current index value can not be marshaled. Index(0) must not
	// return ErrInvalidIndex and there can be no gaps. currently a max of 3 indexes is supported.
	Key() ([]byte, error)

	SetKey(data []byte) error

	// TTL should return the time to live of the record once written.  If this returns 0 the
	// record will not have a time to live set
	TTL() time.Duration

	// Record must return a pointer to the struct representation of this record
	Record() interface{}
}

// ErrNoConfigRecords no record types defined by Config.Records()
var ErrNoConfigRecords = errors.New("record.Config.Records() returned empty slice")

// ErrDupRecordNames more than one record with same name defined by Config.Records()
var ErrDupRecordNames = errors.New("record.Config.Records() returned duplicate record names")

// ErrRecordNameLenNot3 a record with name of length not 3 defined by Config.Records()
var ErrRecordNameLenNot3 = errors.New(
	"record.Config.Records() returned record with name not length 3 in bytes",
)

// ErrNotFound indicates the requested item was not found
var ErrNotFound = errors.New("not found")

// ErrRecordNotDefined indicates a method was called with a record that was not defined in the
// config
var ErrRecordNotDefined = errors.New("Record type used that was not included in config")

type recorderDB struct {
	*badger.DB
	recPrefixes map[string][]byte
	sequences   []*badger.Sequence
	writeChan   chan *badger.Entry
	doneChan    chan<- error
}

// New creates a RecorderDB
// records must have one instance of each record type that will be used in this database.
// New will check that the Name() of all these records are unique.  These records may be used
// as work areas and should be considered owned by this library
func New(store store.Store, records []Record) (RecorderDB, error) {
	if len(records) == 0 {
		return nil, ErrNoConfigRecords
	}
	recPrefixes := make(map[string][]byte, len(records))
	for _, v := range records {
		name := v.Name()
		_, ok := recPrefixes[name]
		if ok {
			return nil, fmt.Errorf("%w name: %v", ErrDupRecordNames, name)
		}
		nameBytes := []byte(name)
		if len(nameBytes) != 3 {
			return nil, fmt.Errorf("%w name: %v", ErrRecordNameLenNot3, name)
		}
		recPrefixes[name] = append(nameBytes, string(0)[0])
	}

	newItem := &recorderDB{
		DB:          store.BadgerDB(),
		recPrefixes: recPrefixes,
		writeChan:   make(chan *badger.Entry, 100),
	}

	go newItem.background()

	return newItem, nil
}

func (r *recorderDB) Close(doneChan chan<- error) {
	r.doneChan = doneChan
	close(r.writeChan)
}

func (r *recorderDB) background() {
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

func (r *recorderDB) close() {
	for _, v := range r.sequences {
		v.Release()
	}
	r.doneChan <- r.DB.Close()
}

func (r *recorderDB) Write(record Record) error {
	name := record.Name()
	prefix, ok := r.recPrefixes[name]
	if !ok {
		return fmt.Errorf("%w name: %v", ErrRecordNotDefined, name)
	}
	keyValue, err := record.Key()
	if err != nil {
		return err
	}
	data, err := json.Marshal(record.Record())
	if err != nil {
		return err
	}
	entry := badger.NewEntry(append(prefix, keyValue...), data)
	ttl := record.TTL()
	if ttl > 0 {
		entry.WithTTL(ttl)
	}
	txn := r.DB.NewTransaction(true)
	err = txn.SetEntry(entry)
	if err != nil {
		txn.Discard()
		return err
	}
	return txn.Commit()
}

func (r *recorderDB) WriteBuffered(record Record) error {
	name := record.Name()
	prefix, ok := r.recPrefixes[name]
	if !ok {
		return fmt.Errorf("%w name: %v", ErrRecordNotDefined, name)
	}
	keyValue, err := record.Key()
	if err != nil {
		return err
	}
	data, err := json.Marshal(record.Record())
	if err != nil {
		return err
	}
	entry := badger.NewEntry(append(prefix, keyValue...), data)
	ttl := record.TTL()
	if ttl > 0 {
		entry.WithTTL(ttl)
	}
	r.writeChan <- entry
	return nil
}

func (r *recorderDB) Read(record Record) error {
	name := record.Name()
	prefix, ok := r.recPrefixes[name]
	if !ok {
		return fmt.Errorf("%w name: %v", ErrRecordNotDefined, name)
	}
	keyValue, err := record.Key()
	if err != nil {
		return err
	}
	txn := r.DB.NewTransaction(false)
	defer txn.Discard()
	item, err := txn.Get(append(prefix, keyValue...))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		return err
	}
	return item.Value(func(val []byte) error {
		return json.Unmarshal(val, record.Record())
	})
}

func (r *recorderDB) Delete(record Record) error {
	name := record.Name()
	prefix, ok := r.recPrefixes[name]
	if !ok {
		return fmt.Errorf("%w name: %v", ErrRecordNotDefined, name)
	}
	keyValue, err := record.Key()
	if err != nil {
		return err
	}
	txn := r.DB.NewTransaction(true)
	err = txn.Delete(append(prefix, keyValue...))
	if err != nil {
		txn.Discard()
		return err
	}
	return txn.Commit()
}

func (r *recorderDB) Range(
	record Record,
	prefixBytes int,
	reverse bool,
	cb func(record Record) bool,
) error {
	name := record.Name()
	prefix, ok := r.recPrefixes[name]
	if !ok {
		return fmt.Errorf("%w name: %v", ErrRecordNotDefined, name)
	}
	keyValue, err := record.Key()
	if err != nil {
		return err
	}
	if prefixBytes >= len(keyValue) {
		return errors.New("prefixBytes as long or longer than key bytes")
	}
	key := append(prefix, keyValue...)
	if prefixBytes > 0 {
		prefix = append(prefix, keyValue[:prefixBytes]...)
	}
	txn := r.DB.NewTransaction(false)
	defer txn.Discard()
	itOps := badger.DefaultIteratorOptions
	itOps.Reverse = reverse
	it := txn.NewIterator(itOps)
	defer it.Close()
	for it.Seek(key); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, record.Record())
		})
		if err != nil {
			return err
		}
		err = record.SetKey(item.Key()[4:])
		if err != nil {
			return err
		}
		if !cb(record) {
			return nil
		}
	}
	return nil
}

func (r *recorderDB) DeletePrefix(record Record, keyPrefix []byte) error {
	name := record.Name()
	prefix, ok := r.recPrefixes[name]
	if !ok {
		return fmt.Errorf("%w name: %v", ErrRecordNotDefined, name)
	}
	return r.DB.DropPrefix(append(prefix, keyPrefix...))
}

func (r *recorderDB) GetSequence(record Record, key []byte) (Sequence, error) {
	name := record.Name()
	prefix, ok := r.recPrefixes[name]
	if !ok {
		return nil, fmt.Errorf("%w name: %v", ErrRecordNotDefined, name)
	}
	seqPrefix := make([]byte, 4)
	if copy(seqPrefix, prefix) != 4 {
		return nil, errors.New("Failed to copy exactly 4 bytes of prefix for GetSequence")
	}
	seqPrefix[3] = 's'
	sequence, err := r.DB.GetSequence(append(seqPrefix, key...), 100)
	if err != nil {
		return nil, err
	}
	r.sequences = append(r.sequences, sequence)
	return sequence, nil
}

func (r *recorderDB) NewTransaction(update bool) RecorderTxn {
	return &recorderTxn{
		Txn:         r.DB.NewTransaction(update),
		recPrefixes: r.recPrefixes,
	}
}
