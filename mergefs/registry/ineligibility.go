package registry

import "github.com/francoispqt/gojay"

// Ineligibility represents a record in the ineligibility table
type Ineligibility struct {
	ID            int   `aerospike:"id,pk" sqlx:"id,primarykey"`
	AudienceID    int   `aerospike:"audienceID,mapKey" sqlx:"audienceID"`
	FeatureTypeID int   `aerospike:"featureTypeId,arrayIndex,arraySize=318" sqlx:"featureTypeId"`
	Value         int64 `aerospike:"value,component" sqlx:"value"`
}

// UnmarshalJSONObject implements gojay's UnmarshalerJSONObject
func (s *Ineligibility) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	switch k {
	case "ID":
		return dec.Int(&s.ID)
	case "AudienceID":
		return dec.Int(&s.AudienceID)
	case "FeatureTypeID":
		return dec.Int(&s.FeatureTypeID)
	case "Value":
		return dec.Int64(&s.Value)
	}
	return nil
}

// NKeys returns the number of keys to unmarshal
func (s *Ineligibility) NKeys() int { return 4 }
