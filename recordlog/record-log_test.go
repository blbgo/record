package recordlog

import (
	"testing"
	"time"

	"github.com/blbgo/testing/assert"
)

func TestLogWrittingAndReading(t *testing.T) {
	a := assert.New(t)

	rl := createRecordLog(a)
	defer closeRecordLog(rl)

	lgrNew, err := rl.New("test log")
	a.NoError(err)

	a.NoError(lgrNew.Log("log message after new"))

	time.Sleep(time.Millisecond * 10)

	count := 0
	var createdSave time.Time
	err = rl.Range(time.Unix(0, 0), false, func(created time.Time, name string) bool {
		a.Equal("test log", name)
		createdSave = created
		count++
		return true
	})
	a.NoError(err)
	a.Equal(1, count)

	lgrOpen, err := rl.Open(createdSave)
	a.NoError(err)

	// must delay a little here to make sure opened log does not write log message with same time
	time.Sleep(200)

	a.NoError(lgrOpen.Log("log message after open"))

	time.Sleep(time.Millisecond * 10)

	count = 0
	err = rl.RangeLog(
		createdSave,
		time.Unix(0, 0),
		false,
		func(created time.Time, message string) bool {
			//a.Equal("log message", message)
			count++
			return true
		},
	)
	a.NoError(err)
	a.Equal(2, count)

	a.NoError(rl.Delete(createdSave))
	_, err = rl.Open(createdSave)
	a.Error(err)

	count = 0
	err = rl.RangeLog(
		createdSave,
		time.Unix(0, 0),
		false,
		func(created time.Time, message string) bool {
			//a.Equal("log message", message)
			count++
			return true
		},
	)
	a.NoError(err)
	a.Equal(0, count)
}
