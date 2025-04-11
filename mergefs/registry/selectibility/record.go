package selectibility

type Record struct {
	ID     int   `aerospike:"id,pk" sqlx:"id,primarykey"`
	Key    int   `aerospike:"key,mapKey" sqlx:"key"`
	Bucket int   `aerospike:"bucket,arrayIndex,arraySize=1440" sqlx:"bucket"`
	Value  int64 `aerospike:"value,component" sqlx:"value"`
}

func NewRecord() interface{} {
	return &Record{}
}
