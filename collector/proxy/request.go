package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/francoispqt/gojay"
	"net/http"
)

type Request struct {
	BatchID string
	Records interface{}
}

func (r *Request) httpRequest(endpoint *Endpoint) (*http.Request, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	shallCompress := endpoint.Content.CompressionSize() > 0 && len(data) > endpoint.Content.CompressionSize()
	if shallCompress {
		if data, err = compressContent(data); err != nil {
			return nil, err
		}
	}
	request, err := http.NewRequest(http.MethodPost, endpoint.URL, bytes.NewReader(data))
	if err == nil {
		request.Header.Set("Content-Type", "application/json")
		if shallCompress {
			request.Header.Set("Content-Encoding", "gzip")
		}
	}
	return request, err
}

// MarshalJSONObject implements MarshalerJSONObject
func (r *Request) MarshalJSONObject(enc *gojay.Encoder) {
	enc.StringKey("BatchID", r.BatchID)
	records, ok := r.Records.(gojay.MarshalerJSONArray)
	if ok {
		enc.ArrayKey("Records", records)
	} else {
		data, _ := json.Marshal(records)
		embeddedJSON := gojay.EmbeddedJSON(data)
		enc.AddEmbeddedJSONKey("Records", &embeddedJSON)
	}
}

// IsNil checks if instance is nil
func (r *Request) IsNil() bool {
	return r == nil
}

// UnmarshalJSONObject implements gojay's UnmarshalerJSONObject
func (r *Request) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	switch k {
	case "BatchID":
		return dec.String(&r.BatchID)

	case "Records":
		aSlice, ok := r.Records.(gojay.UnmarshalerJSONArray)
		if ok {
			return dec.Array(aSlice)
		}
		return fmt.Errorf("unsupported Records type, %T", r.Records)
	}
	return nil
}

// NKeys returns the number of keys to unmarshal
func (r *Request) NKeys() int { return 0 }
