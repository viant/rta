package registry

import (
	"github.com/viant/x"
	"reflect"
)

func init() {
	registry.Register(x.NewType(reflect.TypeOf(Signal{}), x.WithName("Signal")))
	registry.Register(x.NewType(reflect.TypeOf(Selectibility{}), x.WithName("Selectibility")))
	registry.Register(x.NewType(reflect.TypeOf(Ineligibility{}), x.WithName("Ineligibility")))
}
