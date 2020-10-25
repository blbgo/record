package recordstate

import (
	"github.com/blbgo/general"
	"github.com/blbgo/record/record"
)

type recordState struct {
	recorderDB record.RecorderDB
}

// New creates a PersistentState implemented by recordState
func New(recorderDB record.RecorderDB) general.PersistentState {
	return &recordState{recorderDB: recorderDB}
}

// NewRecords returns an instance of each record type in this package
func NewRecords() record.Record {
	return &stateEntry{}
}

// **************** implement PersistentState

func (r *recordState) Save(name string, state interface{}) error {
	newState := &stateEntry{
		key:   name,
		state: state,
	}
	err := r.recorderDB.Write(newState)
	if err != nil {
		return err
	}
	return nil
}

func (r *recordState) Retrieve(name string, state interface{}) error {
	newState := &stateEntry{
		key:   name,
		state: state,
	}
	err := r.recorderDB.Read(newState)
	if err != nil {
		return err
	}
	return nil
}
