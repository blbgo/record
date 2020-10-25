package recordlog

import (
	"time"

	"github.com/blbgo/record/record"
)

type logEntry struct {
	ttl time.Duration

	logKey time.Time
	entryKey time.Time

	Message string
}

// **************** implement record.Record

var logEntryRecordName = "lge"

func (r *logEntry) Name() string {
	return logEntryRecordName
}

func (r *logEntry) Key() ([]byte, error) {
	return append(record.TimeToBytes(r.logKey), record.TimeToBytes(r.entryKey)...), nil
}

func (r *logEntry) SetKey(data []byte) error {
	logKey, err := record.BytesToTime(data[:12])
	if err != nil {
		return err
	}
	
	entryKey, err := record.BytesToTime(data[12:])
	if err != nil {
		return err
	}

	r.logKey = logKey
	r.entryKey = entryKey

	return nil
}

func (r *logEntry) TTL() time.Duration {
	return r.ttl
}

func (r *logEntry) Record() interface{} {
	return r
}
