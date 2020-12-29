package rootlog

import (
	"sync"
	"time"

	"github.com/blbgo/general"
	"github.com/blbgo/record/root"
)

// RootLog interface allows managing named logs
type RootLog interface {
	general.LoggerFactory

	Open(created time.Time) (general.Logger, error)

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

type rootLog struct {
	rootLogItem root.Item
	mutex       sync.Mutex
	lastTime    time.Time
	counter     byte
}

// New creates a RootLog
func New(rootDB root.Root) (RootLog, general.LoggerFactory, error) {
	item, err := rootDB.RootItem(
		"github.com/blbgo/record/rootlog",
		"github.com/blbgo/record/rootlog root item",
	)
	if err != nil {
		return nil, nil, err
	}
	r := &rootLog{rootLogItem: item}
	return r, r, nil

}

// **************** implement RootLog

func (r *rootLog) New(name string) (general.Logger, error) {
	created := r.makeLogKey()
	itemLog, err := r.rootLogItem.CreateChild(TimeToBytes(created), []byte(name), nil)
	if err != nil {
		return nil, err
	}
	return &log{itemLog: itemLog}, nil
}

func (r *rootLog) Open(created time.Time) (general.Logger, error) {
	itemLog, err := r.rootLogItem.ReadChild(TimeToBytes(created))
	if err != nil {
		return nil, err
	}
	return &log{itemLog: itemLog}, nil
}

func (r *rootLog) Delete(created time.Time) error {
	itemLog, err := r.rootLogItem.ReadChild(TimeToBytes(created))
	if err != nil {
		return err
	}
	err = itemLog.DeleteChildren()
	if err != nil {
		return err
	}
	return itemLog.Delete()
}

func (r *rootLog) Range(
	start time.Time,
	reverse bool,
	cb func(created time.Time, name string) bool,
) error {
	var cbErr error
	err := r.rootLogItem.RangeChildren(TimeToBytes(start), 0, reverse, func(item root.Item) bool {
		var created time.Time
		created, cbErr = BytesToTime(item.CopyKey(nil))
		if cbErr != nil {
			return false
		}
		return cb(created, string(item.Value()))
	})
	if err != nil {
		return err
	}
	return cbErr
}

func (r *rootLog) RangeLog(
	logCreated time.Time,
	start time.Time,
	reverse bool,
	cb func(created time.Time, message string) bool,
) error {
	itemLog, err := r.rootLogItem.ReadChild(TimeToBytes(logCreated))
	if err != nil {
		return err
	}
	var cbErr error
	err = itemLog.RangeChildren(TimeToBytes(start), 0, reverse, func(item root.Item) bool {
		var created time.Time
		created, cbErr = BytesToTime(item.CopyKey(nil))
		if cbErr != nil {
			return false
		}
		return cb(created, string(item.Value()))
	})
	if err != nil {
		return err
	}
	return cbErr
}

// **************** helpers

func (r *rootLog) makeLogKey() time.Time {
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
