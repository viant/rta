package signal

import (
	"fmt"
	"strconv"
	"sync"
)

func Key(record interface{}) interface{} {
	if record == nil {
		return ""
	}
	src := record.(*Record)
	if src.Value == nil {
		return ""
	}

	switch actual := src.Value.(type) {
	case string:
		return stringKey(src.ID, src.Bucket, actual)
	case int:
		return stringKey(src.ID, src.Bucket, strconv.Itoa(actual))
	case int32:
		return stringKey(src.ID, src.Bucket, strconv.Itoa(int(actual)))
	case int64:
		return stringKey(src.ID, src.Bucket, strconv.Itoa(int(actual)))
	case float64: // encode/json uses float64 for numbers
		return stringKey(src.ID, src.Bucket, strconv.Itoa(int(actual)))
	default:
		panic(fmt.Sprintf("unsupported type %T (value: %v)", src.Value, src.Value))
	}
}

const (
	// max digits in an unsigned 64-bit decimal + 1 for '/'
	maxKeyPrefix = 20 + 1
)

var stringKeyPool = sync.Pool{
	New: func() interface{} {
		// start with a buffer that's hopefully large enough for most uses
		return make([]byte, 0, 64)
	},
}

// stringKey combines the upper 32 bits of `id` with `bucket` in the low 32 bits,
// then appends "/" + value.  Reuses the buffer via sync.Pool.
func stringKey(id, bucket int, value string) string {
	// clear low 32 bits of id, then OR in bucket
	key := (uint64(id) &^ 0xFFFFFFFF) | uint64(uint32(bucket))

	// grab and reset a buffer
	buf := stringKeyPool.Get().([]byte)
	buf = buf[:0]

	// ensure capacity for digits + '/' + value
	needed := maxKeyPrefix + len(value)
	if cap(buf) < needed {
		buf = make([]byte, 0, needed)
	}

	// write the decimal key
	buf = strconv.AppendUint(buf, key, 10)
	buf = append(buf, '/')
	buf = append(buf, value...)

	// convert to string (one allocation) and return buffer to pool
	s := string(buf)
	stringKeyPool.Put(buf)
	return s
}
