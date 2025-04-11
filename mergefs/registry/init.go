package registry

import (
	"github.com/viant/rta/mergefs/registry/ineligibility"
	"github.com/viant/rta/mergefs/registry/selectibility"
	"github.com/viant/rta/mergefs/registry/signal"
	"github.com/viant/x"
	"reflect"
)

var TypeRegistry = x.NewRegistry()

func init() {
	registerRecord(signal.Record{}, signal.Key, signal.Reduce, signal.Mapper, signal.NewRecord)
	registerRecord(selectibility.Record{}, selectibility.Key, selectibility.Reduce, selectibility.Mapper, selectibility.NewRecord)
	registerRecord(ineligibility.Record{}, ineligibility.Key, ineligibility.Reduce, ineligibility.Mapper, ineligibility.NewRecord)
}

func registerRecord(record interface{}, key keyFn, reduce reduceFn, mapper mapperFn, new newFn) {
	rType := reflect.TypeOf(record)
	typeName := fullTypeName(rType)

	TypeRegistry.Register(x.NewType(rType, x.WithName(rType.Name())))
	keyFnRegistry.Register(typeName, key)
	reduceFnRegistry.Register(typeName, reduce)
	mapperFnRegistry.Register(typeName, mapper)
	newFnRegistry.Register(typeName, new)
}

func fullTypeName(t reflect.Type) string {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.PkgPath() + "." + t.Name()
}
