package signal_test

import (
	"strconv"
	"testing"
)

func BenchmarkStringKey1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = stringKey1(0x1234567800000000, 42, 123)
	}
}

// BenchmarkStringKey1-16    	10180380	       111.3 ns/op

func BenchmarkStringKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = stringKey2(0x1234567800000000, 42, 123)
	}
}

// BenchmarkStringKey-16    	12599797	        84.54 ns/op

func stringKey1(id int, mapKey int, bucket int) interface{} {
	key := uint64(id) & 0xFFFFFFFF00000000 // clear low 32 bits (not needed info about order id)
	key |= uint64(uint32(bucket))          // set new lower 32 bits from bucket

	// the max number of digits in a uint64 is 20
	buf := make([]byte, 0, 41) // 20 digits + 1 separator + 20 digits
	buf = strconv.AppendUint(buf, key, 10)
	buf = append(buf, '/')
	buf = strconv.AppendInt(buf, int64(mapKey), 10)

	return string(buf)
}

func stringKey2(id int, mapKey int, bucket int) interface{} {
	compositeKey := int64(id) & 0xFFFFFFFF00000000 // clear low 32 bits (not needed info about order id)
	compositeKey |= int64(bucket)

	compositeKeyLiteral := strconv.FormatInt(compositeKey, 10)
	keyLiteral := strconv.Itoa(mapKey)

	result := make([]byte, len(compositeKeyLiteral)+1+len(keyLiteral))
	m := copy(result, compositeKeyLiteral)
	result[m] = '/'
	m++
	copy(result[m:], keyLiteral)
	return string(result)
}
