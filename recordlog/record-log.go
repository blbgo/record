package recordlog

import (
	"errors"
	"sync"
	"time"

	"github.com/blbgo/general"
	"github.com/blbgo/record/record"
)

// RecordLog interface allows managing named logs
type RecordLog interface {
	general.LoggerFactory

	Open(created time.Time) (general.Logger, error)

	SetTTL(generalLog general.Logger, ttl time.Duration) error

	Delete(created time.Time) error

	Range(
		start time.Time,
		reverse bool,
		cb func(created time.Time, name string) bool,
	) error

	RangeLog(
		logCreated time.Time,
		start time.Time,
		reverse bool,
		cb func(created time.Time, message string) bool,
	) error
}

var minTime = time.Unix(0, 0).UTC()

// MinTime returns the zero or minimum time
func MinTime() time.Time {
	return minTime
}

var maxTime = time.Unix(1<<63-62135596801, 999999999).UTC()

// MaxTime returns the maximum time in go
func MaxTime() time.Time {
	return maxTime
}

type recordLog struct {
	recorderDB record.RecorderDB
	mutex      sync.Mutex
	lastTime   time.Time
	counter    byte
}

// New creates a RecordLog
func New(recorderDB record.RecorderDB) RecordLog {
	return &recordLog{recorderDB: recorderDB}

}

// NewRecords returns an instance of each record type in this package
func NewRecords() (record.Record, record.Record) {
	return &log{}, &logEntry{}
}

// **************** implement RecordLog

func (r *recordLog) New(name string) (general.Logger, error) {
	newLog := &log{
		recorderDB: r.recorderDB,
		created:    r.makeLogKey(),
		LogName:    name,
	}
	err := r.recorderDB.Write(newLog)
	if err != nil {
		return nil, err
	}
	return newLog, nil
}

func (r *recordLog) Open(created time.Time) (general.Logger, error) {
	openLog := &log{created: created}
	err := r.recorderDB.Read(openLog)
	if err != nil {
		return nil, err
	}
	openLog.recorderDB = r.recorderDB
	return openLog, nil
}

func (r *recordLog) SetTTL(generalLog general.Logger, ttl time.Duration) error {
	recLog, ok := generalLog.(*log)
	if !ok {
		return errors.New("recordlog.SetTTL called with general.Logger that is not recordlog.log")
	}
	recLog.entryTTL = ttl
	return nil
}

func (r *recordLog) Delete(created time.Time) error {
	// verify a log with the specified time exists
	deleteLog := &log{created: created}
	err := r.recorderDB.Read(deleteLog)
	if err != nil {
		return err
	}

	// next delete all log entries for this log
	// unfortunately this can not be done in a transaction
	err = r.recorderDB.DeletePrefix(&logEntry{}, record.TimeToBytes(created))
	if err != nil {
		return err
	}

	// last delete the log
	err = r.recorderDB.Delete(deleteLog)
	if err != nil {
		return err
	}
	return nil
}

func (r *recordLog) Range(
	start time.Time,
	reverse bool,
	cb func(created time.Time, name string) bool,
) error {
	rangeLog := &log{created: start}

	return r.recorderDB.Range(
		rangeLog,
		0,
		reverse,
		func(record record.Record) bool {
			return cb(rangeLog.created, rangeLog.LogName)
		},
	)
}

func (r *recordLog) RangeLog(
	logCreated time.Time,
	start time.Time,
	reverse bool,
	cb func(created time.Time, message string) bool,
) error {
	rangeLogEntry := &logEntry{logKey: logCreated, entryKey: start}
	return r.recorderDB.Range(
		rangeLogEntry,
		record.TimeBytesLength,
		reverse,
		func(record record.Record) bool {
			return cb(rangeLogEntry.entryKey, rangeLogEntry.Message)
		},
	)
}

// **************** helpers

func (r *recordLog) makeLogKey() time.Time {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	t := time.Now().UTC()
	if t == r.lastTime {
		r.counter++
		return time.Unix(t.Unix(), int64(t.Nanosecond())+int64(r.counter))
	}
	r.lastTime = t
	r.counter = 0
	return t
}
