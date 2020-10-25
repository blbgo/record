package recordstate

import (
	"time"
)

type stateEntry struct {
	key string

	state interface{}
}

// **************** implement record.Record

var stateRecordName = "ste"

func (r *stateEntry) Name() string {
	return stateRecordName
}

func (r *stateEntry) Key() ([]byte, error) {
	return []byte(r.key), nil
}

func (r *stateEntry) SetKey(data []byte) error {
	r.key = string(data)

	return nil
}

func (r *stateEntry) TTL() time.Duration {
	return 0
}

func (r *stateEntry) Record() interface{} {
	return r.state
}
