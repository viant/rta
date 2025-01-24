package registry

import (
	"github.com/francoispqt/gojay"
)

// Selectibility represents a record in the selectibility table
type Selectibility struct {
	ID     int   `aerospike:"id,pk" sqlx:"id,primarykey"`
	Key    int   `aerospike:"key,mapKey" sqlx:"key"`
	Bucket int   `aerospike:"bucket,arrayIndex,arraySize=1440" sqlx:"bucket"`
	Value  int64 `aerospike:"value,component" sqlx:"value"`
}

// UnmarshalJSONObject implements gojay's UnmarshalerJSONObject
func (s *Selectibility) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	switch k {
	case "ID":
		return dec.Int(&s.ID)
	case "Key":
		return dec.Int(&s.Key)
	case "Bucket":
		return dec.Int(&s.Bucket)
	case "Value":
		return dec.Int64(&s.Value)
	}
	return nil
}

// NKeys returns the number of keys to unmarshal
func (s *Selectibility) NKeys() int { return 4 }
