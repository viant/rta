package ineligibility

import (
	"strconv"
)

func Key(record interface{}) interface{} {
	if record == nil {
		return ""
	}
	src := record.(*Record)
	return stringKey(src.ID, src.AudienceID, src.FeatureTypeID)
}

func stringKey(id int, mapKey int, bucket int) interface{} {
	key := uint64(id) & 0xFFFFFFFF00000000 // clear low 32 bits (not needed info about order id)
	key |= uint64(uint32(bucket))          // set new lower 32 bits from bucket

	// the max number of digits in a uint64 is 20
	buf := make([]byte, 0, 41) // 20 digits + 1 separator + 20 digits
	buf = strconv.AppendUint(buf, key, 10)
	buf = append(buf, '/')
	buf = strconv.AppendInt(buf, int64(mapKey), 10)

	return string(buf)
}
