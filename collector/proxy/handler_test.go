package proxy

import (
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync"
	"testing"
)

func TestHandler_Handle(t *testing.T) {

	var testCases = []struct {
		description string
		data        string
		collector   *recordCollector
		expect      map[string]float32
	}{
		{
			description: "simple agg",
			data: `{
"BatchID":"123",
"Records":[
	{"Key":"1", "Amount":1.2},
	{"Key":"2", "Amount":2.2},
	{"Key":"3", "Amount":3.3},
	{"Key":"1", "Amount":10.2},
	{"Key":"3", "Amount":0.2}
]
}`,
			expect: map[string]float32{
				"1": 11.4,
				"2": 2.2,
				"3": 3.5,
			},
			collector: &recordCollector{collection: map[string]float32{}},
		},
		{
			description: "simple agg empty data",
			data: `{
"BatchID":"123",
"Records":[]
}`,
			expect:    map[string]float32{},
			collector: &recordCollector{collection: map[string]float32{}},
		},
		{
			description: "simple agg 1 row",
			data: `{
"BatchID":"123",
"Records":[
	{"Key":"1", "Amount":1.2},
]
}`,
			expect: map[string]float32{
				"1": 1.2,
			},
			collector: &recordCollector{collection: map[string]float32{}},
		},
		{
			description: "simple agg 4 rows ",
			data: `{
"BatchID":"123",
"Records":[
	{"Key":"1", "Amount":1.2},
	{"Key":"2", "Amount":2.2},
	{"Key":"3", "Amount":3.3},
	{"Key":"1", "Amount":10.2}
]
}`,
			expect: map[string]float32{
				"1": 11.4,
				"2": 2.2,
				"3": 3.3,
			},
			collector: &recordCollector{collection: map[string]float32{}},
		},
		{
			description: "simple agg 7 rows ",
			data: `{
"BatchID":"123",
"Records":[
	{"Key":"1", "Amount":1.2},
	{"Key":"2", "Amount":2.2},
	{"Key":"3", "Amount":3.3},
	{"Key":"1", "Amount":9.2},
	{"Key":"1", "Amount":1.1},
	{"Key":"2", "Amount":2.2},
	{"Key":"3", "Amount":3.3}
]
}`,
			expect: map[string]float32{
				"1": 11.5,
				"2": 4.4,
				"3": 6.6,
			},
			collector: &recordCollector{collection: map[string]float32{}},
		},
		{
			description: "simple agg 15 rows ",
			data: `{
"BatchID":"123",
"Records":[
	{"Key":"1", "Amount":1.2},
	{"Key":"2", "Amount":2.2},
	{"Key":"3", "Amount":3.3},
	{"Key":"1", "Amount":10.2},
	{"Key":"1", "Amount":1.2},
	{"Key":"2", "Amount":2.2},
	{"Key":"3", "Amount":3.3},
	{"Key":"1", "Amount":10.2},
	{"Key":"1", "Amount":1.2},
	{"Key":"2", "Amount":2.2},
	{"Key":"3", "Amount":3.3},
	{"Key":"1", "Amount":10.2},
	{"Key":"1", "Amount":1.2},
	{"Key":"2", "Amount":2.2},
	{"Key":"3", "Amount":3.3}
]
}`,
			expect: map[string]float32{
				"1": 35.4,
				"2": 8.8,
				"3": 13.2,
			},
			collector: &recordCollector{collection: map[string]float32{}},
		},
	}

	for _, testCase := range testCases {
		handler := NewHandler(reflect.TypeOf(records{}), testCase.collector)
		err := handler.Handle([]byte(testCase.data))
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.expect, testCase.collector.collection, testCase.description)
	}

}

type (
	record struct {
		Key    string
		Amount float32
	}

	records         []*record
	recordCollector struct {
		mux        sync.Mutex
		collection map[string]float32
	}
)

func (c *recordCollector) Collect(rec interface{}) error {
	aRecord, ok := rec.(*record)
	if !ok {
		return fmt.Errorf("expected: %T, but had: %T", aRecord, rec)
	}
	c.mux.Lock()
	defer c.mux.Unlock()

	if len(c.collection) == 0 {
		c.collection = map[string]float32{}
	}
	c.collection[aRecord.Key] += aRecord.Amount
	return nil
}

func (s *records) UnmarshalJSONArray(dec *gojay.Decoder) error {
	var value = &record{}
	if err := dec.Object(value); err != nil {
		return err
	}
	*s = append(*s, value)
	return nil
}

// UnmarshalJSONObject implements gojay's UnmarshalerJSONObject
func (r *record) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	switch k {
	case "Key":
		return dec.String(&r.Key)
	case "Amount":
		return dec.Float32(&r.Amount)
	}
	return nil
}

// NKeys returns the number of keys to unmarshal
func (r *record) NKeys() int { return 0 }
