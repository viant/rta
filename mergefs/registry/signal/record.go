package signal

type Record struct {
	ID     int         `aerospike:"id,pk" sqlx:"id" json:"ID"`
	Value  interface{} `aerospike:"value,mapKey" sqlx:"value" json:"Value"`
	Bucket int         `aerospike:"bucket,arrayIndex,arraySize=144" sqlx:"bucket" json:"Bucket"`
	Count  int64       `aerospike:"count,component" sqlx:"count" json:"Count"`
}

func NewRecord() interface{} {
	return &Record{}
}
