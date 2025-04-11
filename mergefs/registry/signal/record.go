package signal

type Record struct {
	ID     int         `aerospike:"id,pk" sqlx:"id"`
	Value  interface{} `aerospike:"value,mapKey" sqlx:"value"`
	Bucket int         `aerospike:"bucket,arrayIndex,arraySize=144" sqlx:"bucket"`
	Count  int64       `aerospike:"count,component" sqlx:"count"`
}

func NewRecord() interface{} {
	return &Record{}
}
