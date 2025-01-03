package extension

import (
	"github.com/viant/x"
	"reflect"
)

func init() {
	registry.Register(x.NewType(reflect.TypeOf(Signal{}), x.WithName("Signal")))
}
