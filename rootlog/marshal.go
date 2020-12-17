package rootlog

import (
	"errors"
	"time"
)

// TimeBytesLength is the byte count of the array returned by TimeToBytes
const TimeBytesLength = 12

// TimeToBytes convert time.Time to []byte in a sortable way
func TimeToBytes(t time.Time) []byte {
	sec := t.Unix()
	nsec := t.Nanosecond()
	return []byte{
		byte(sec >> 56), // bytes 1-8: seconds
		byte(sec >> 48),
		byte(sec >> 40),
		byte(sec >> 32),
		byte(sec >> 24),
		byte(sec >> 16),
		byte(sec >> 8),
		byte(sec),
		byte(nsec >> 24), // bytes 9-12: nanoseconds
		byte(nsec >> 16),
		byte(nsec >> 8),
		byte(nsec),
	}
}

// BytesToTime converts a []byte to time.Time
func BytesToTime(data []byte) (time.Time, error) {
	if len(data) != /*sec*/ 8+ /*nsec*/ 4 {
		return time.Unix(0, 0), errors.New("record.BytesToTime: invalid length")
	}
	sec := int64(data[7]) | int64(data[6])<<8 | int64(data[5])<<16 | int64(data[4])<<24 |
		int64(data[3])<<32 | int64(data[2])<<40 | int64(data[1])<<48 | int64(data[0])<<56

	nsec := int32(data[11]) | int32(data[10])<<8 | int32(data[9])<<16 | int32(data[8])<<24

	return time.Unix(sec, int64(nsec)), nil
}
