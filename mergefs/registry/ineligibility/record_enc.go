package ineligibility

import (
	"github.com/francoispqt/gojay"
)

// MarshalJSONObject implements MarshalerJSONObject
func (r *Record) MarshalJSONObject(enc *gojay.Encoder) {
	enc.IntKey("ID", r.ID)
	enc.IntKey("AudienceID", r.AudienceID)
	enc.IntKey("FeatureTypeID", r.FeatureTypeID)
	enc.Int64Key("Value", r.Value)
}

// IsNil checks if instance is nil
func (r *Record) IsNil() bool {
	return r == nil
}

// UnmarshalJSONObject implements gojay's UnmarshalerJSONObject
func (r *Record) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {

	switch k {
	case "ID":
		return dec.Int(&r.ID)

	case "AudienceID":
		return dec.Int(&r.AudienceID)

	case "FeatureTypeID":
		return dec.Int(&r.FeatureTypeID)

	case "Value":
		return dec.Int64(&r.Value)

	}
	return nil
}

// NKeys returns the number of keys to unmarshal
func (r *Record) NKeys() int { return 4 }

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
