package ineligibility

type Record struct {
	ID            int   `aerospike:"id,pk" sqlx:"id,primarykey"`
	AudienceID    int   `aerospike:"audienceID,mapKey" sqlx:"audienceID"`
	FeatureTypeID int   `aerospike:"featureTypeId,arrayIndex,arraySize=318" sqlx:"featureTypeId"`
	Value         int64 `aerospike:"value,component" sqlx:"value"`
}

func NewRecord() interface{} {
	return &Record{}
}
