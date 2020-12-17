package rootlog

import (
	"fmt"
	"sync"
	"time"

	"github.com/blbgo/record/root"
)

type log struct {
	itemLog  root.Item
	mutex    sync.Mutex
	lastTime time.Time
	counter  byte
}

// **************** implement general.Logger

func (r *log) Log(v ...interface{}) error {
	created := r.makeEntryKey()
	return r.itemLog.QuickChild(TimeToBytes(created), []byte(fmt.Sprint(v...)))
}

func (r *log) Logf(format string, v ...interface{}) error {
	created := r.makeEntryKey()
	return r.itemLog.QuickChild(TimeToBytes(created), []byte(fmt.Sprintf(format, v...)))
}

// **************** helpers

func (r *log) makeEntryKey() time.Time {
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
