package record

import (
	"encoding/json"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
)

// RecorderTxn is an interface to a transaction created by RecorderDB.NewTransaction.  Discard or
// Commit must be called to end teh transaction.  There is no harm in calling Discard after Commit
// so a good pattern is to defer Discard() right after RecorderDB.NewTransaction
type RecorderTxn interface {
	Recorder

	Discard()
	Commit() error
}

type recorderTxn struct {
	*badger.Txn
	recPrefixes map[string][]byte
}

func (r *recorderTxn) Write(record Record) error {
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
	return r.SetEntry(entry)
}

func (r *recorderTxn) Read(record Record) error {
	name := record.Name()
	prefix, ok := r.recPrefixes[name]
	if !ok {
		return fmt.Errorf("%w name: %v", ErrRecordNotDefined, name)
	}
	keyValue, err := record.Key()
	if err != nil {
		return err
	}
	item, err := r.Get(append(prefix, keyValue...))
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

func (r *recorderTxn) Delete(record Record) error {
	name := record.Name()
	prefix, ok := r.recPrefixes[name]
	if !ok {
		return fmt.Errorf("%w name: %v", ErrRecordNotDefined, name)
	}
	keyValue, err := record.Key()
	if err != nil {
		return err
	}
	return r.Txn.Delete(append(prefix, keyValue...))
}

func (r *recorderTxn) Range(record Record, prefixBytes int, reverse bool, cb func(record Record) bool) error {
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
	itOps := badger.DefaultIteratorOptions
	itOps.Reverse = reverse
	it := r.NewIterator(itOps)
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
