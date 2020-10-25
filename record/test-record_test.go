package record

import (
	"time"
)

type testRecord struct {
	// Key field will not be part of main record due to `json:"-"`
	// non public fields will also not be included
	KeyField time.Time `json:"-"`

	FirstName string
	Age       int
}

var testRecordName = "tre"

func (r *testRecord) Name() string {
	return testRecordName
}

func (r *testRecord) Key() ([]byte, error) {
	return TimeToBytes(r.KeyField), nil
}

func (r *testRecord) SetKey(data []byte) error {
	t, err := BytesToTime(data)
	if err != nil {
		return err
	}

	r.KeyField = t

	return nil
}

func (r *testRecord) TTL() time.Duration {
	return 0
}

func (r *testRecord) Record() interface{} {
	return r
}
