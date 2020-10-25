package recordlog

import (
	"github.com/blbgo/testing/assert"

	"github.com/blbgo/general"
	"github.com/blbgo/record/record"
	"github.com/blbgo/record/store"
)

// Records is an array of record.Record used by this package to help with config of record
var Records = []record.Record{
	&log{},
	&logEntry{},
}

type recordConfig struct{}

func (r *recordConfig) Records() []record.Record {
	return Records
}

func createRecordLog(a *assert.Assert) RecordLog {
	st, err := store.New(store.NewConfigInMem())
	a.NoError(err)
	a.NotNil(st)

	db, err := record.New(st, Records)
	a.NoError(err)
	a.NotNil(db)

	rl := New(db)
	a.NotNil(rl)

	return rl
}

func closeRecordLog(rl RecordLog) {
	doneChan := make(chan error)
	rl.(*recordLog).recorderDB.(general.DelayCloser).Close(doneChan)
	<-doneChan
}
