package signal

import (
	"bytes"
	"encoding/json"
	"github.com/francoispqt/gojay"
	"strconv"
)

// MarshalJSONObject implements MarshalerJSONObject
func (s *Record) MarshalJSONObject(enc *gojay.Encoder) {
	enc.IntKey("id", s.ID)
	switch val := s.Value.(type) {
	case string:
		enc.StringKey("value", val)
	default:
		enc.IntKey("value", val.(int))
	}
	enc.IntKey("bucket", s.Bucket)
	enc.IntKey("count", int(s.Count))
}

// IsNil checks if instance is nil
func (s *Record) IsNil() bool {
	return s == nil
}

// UnmarshalJSONObject implements gojay's UnmarshalerJSONObject
func (s *Record) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
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
func (s *Record) NKeys() int { return 4 }

type Records []*Record

func (s *Records) UnmarshalJSONArray(dec *gojay.Decoder) error {
	var value = &Record{}
	if err := dec.Object(value); err != nil {
		return err
	}
	*s = append(*s, value)
	return nil
}

func (s Records) MarshalJSONArray(enc *gojay.Encoder) {
	for i := 0; i < len(s); i++ {
		enc.Object(s[i])
	}
}

func (s *Records) IsNil() bool {
	return s == nil
}

type IRecords []interface{}

func (s *IRecords) UnmarshalJSONArray(dec *gojay.Decoder) error {
	var value = &Record{}
	if err := dec.Object(value); err != nil {
		return err
	}
	*s = append(*s, value)
	return nil
}
