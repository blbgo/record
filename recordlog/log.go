package recordlog


import (
	"fmt"
	"sync"
	"time"

	"github.com/blbgo/record/record"
)

type log struct {
	recorderDB record.RecorderDB
	mutex sync.Mutex
	lastTime time.Time
	counter byte
	entryTTL time.Duration

	// created is when the log was created.  It is the record key and will not be part of main
	// record.
	created time.Time

	LogName string
}

// **************** implement record.Record

var logRecordName = "log"

func (r *log) Name() string {
	return logRecordName
}

func (r *log) Key() ([]byte, error) {
	return record.TimeToBytes(r.created), nil
}

func (r *log) SetKey(data []byte) error {
	t, err := record.BytesToTime(data)
	if err != nil {
		return err
	}

	r.created = t

	return nil
}

func (r *log) TTL() time.Duration {
	return 0
}

func (r *log) Record() interface{} {
	return r
}

// **************** implement general.Logger

func (r *log) Log(v ...interface{}) error {
	return r.recorderDB.WriteBuffered(
		&logEntry{
			ttl: r.entryTTL,
			logKey: r.created,
			entryKey: r.makeEntryKey(),
			Message: fmt.Sprint(v...),
		},
	)
}

func (r *log) Logf(format string, v ...interface{}) error {
	return r.recorderDB.WriteBuffered(
		&logEntry{
			ttl: r.entryTTL,
			logKey: r.created,
			entryKey: r.makeEntryKey(),
			Message: fmt.Sprintf(format, v...),
		},
	)
}

// **************** helpers

func (r *log) makeEntryKey() time.Time {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	t := time.Now().UTC()
	if t == r.lastTime {
		r.counter++
		return time.Unix(t.Unix(), int64(t.Nanosecond()) + int64(r.counter))
	}
	r.lastTime = t
	r.counter = 0
	return t
}
