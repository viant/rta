package ineligibility

type Record struct {
	ID            int   `aerospike:"id,pk" sqlx:"id,primarykey" json:"ID"`
	AudienceID    int   `aerospike:"audienceID,mapKey" sqlx:"audienceID" json:"AudienceID"`
	FeatureTypeID int   `aerospike:"featureTypeId,arrayIndex,arraySize=318" sqlx:"featureTypeId" json:"FeatureTypeID"`
	Value         int64 `aerospike:"value,component" sqlx:"value" json:"Value"`
}

func NewRecord() interface{} {
	return &Record{}
}
