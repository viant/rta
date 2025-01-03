package extension

import (
	"github.com/viant/x"
	"github.com/viant/xreflect"
	"reflect"
)

func init() {
	aType := x.NewType(reflect.TypeOf(Signal{}), x.WithName("Signal"))
	registry.Register(aType)
}
