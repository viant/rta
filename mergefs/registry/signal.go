package registry

import (
	"bytes"
	"encoding/json"
	"github.com/francoispqt/gojay"
	"strconv"
)

// Signal represents a record in the signal table
type Signal struct {
	ID     int         `aerospike:"id,pk" sqlx:"id"`
	Value  interface{} `aerospike:"value,mapKey" sqlx:"value"`
	Bucket int         `aerospike:"bucket,arrayIndex,arraySize=144" sqlx:"bucket"`
	Count  int64       `aerospike:"count,component" sqlx:"count"`
}

// UnmarshalJSONObject implements gojay's UnmarshalerJSONObject
func (s *Signal) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	switch k {
	case "ID":
		return dec.Int(&s.ID)
	case "Bucket":
		return dec.Int(&s.Bucket)
	case "Count":
		return dec.Int64(&s.Count)
	case "Value":
		var bs gojay.EmbeddedJSON
		err := dec.EmbeddedJSON(&bs)
		if err != nil {
			return err
		}

		if bytes.HasPrefix(bs, []byte("\"")) {
			err = json.Unmarshal(bs, &s.Value)
			if err != nil {
				return err
			}
		} else {
			switch string(bs) {
			case "true":
				s.Value = true
			case "false":
				s.Value = false
			case "null":
				s.Value = nil
			default:
				if bytes.Contains(bs, []byte(".")) {
					s.Value, err = strconv.ParseFloat(string(bs), 64)
				} else {
					s.Value, err = strconv.ParseInt(string(bs), 10, 64)
				}

				if err != nil {
					return err
				}
			}
		}

		if floatValue, ok := s.Value.(float64); ok {
			s.Value = int(floatValue)
		}
	}
	return nil
}

// NKeys returns the number of keys to unmarshal
func (s *Signal) NKeys() int { return 4 }
