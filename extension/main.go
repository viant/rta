package extension

import (
	"github.com/viant/x"
	"reflect"
	"time"
)

func init() {
	aType := x.NewType(reflect.TypeOf(Signal2{}), x.WithName("Signal"))
	registry.Register(aType)
	//registry.Register(x.NewType(reflect.TypeOf(Signal2{}), x.WithName("Signal2")))
}

type Signal2 struct {
	ID     int         `aerospike:"id,pk" sqlx:"id"`
	Value  interface{} `aerospike:"value,mapKey" sqlx:"value"`
	Bucket int         `aerospike:"bucket,arrayIndex,arraySize=144" sqlx:"bucket"`
	Count  int64       `aerospike:"count,component" sqlx:"count"`
	At     *time.Time  `aerospike:"-" sqlx:"-"`
}
