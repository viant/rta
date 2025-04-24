package selectibility

type Record struct {
	ID     int   `aerospike:"id,pk" sqlx:"id,primarykey" json:"ID"`
	Key    int   `aerospike:"key,mapKey" sqlx:"key" json:"Key"`
	Bucket int   `aerospike:"bucket,arrayIndex,arraySize=1440" sqlx:"bucket" json:"Bucket"`
	Value  int64 `aerospike:"value,component" sqlx:"value" json:"Value"`
}

func NewRecord() interface{} {
	return &Record{}
}
