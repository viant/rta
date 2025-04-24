package signal

import (
	"fmt"
	"strconv"
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

func stringKey(id int, bucket int, value string) string {
	key := uint64(id) & 0xFFFFFFFF00000000 // keep upper 32 bits (dayKey), clear lower (modulo value result)
	key |= uint64(uint32(bucket))          // set new lower 32 bits from bucket

	// the max number of digits in a uint64 is 20
	buf := make([]byte, 0, 21+len(value)) // 20 digits + 1 separator + 20 digits
	buf = strconv.AppendUint(buf, key, 10)
	buf = append(buf, '/')
	buf = append(buf, value...)

	return string(buf)
}
