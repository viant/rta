package extension

import (
	"github.com/viant/x"
)

var registry = x.NewRegistry() //TODO

// Register registers a type with options
func Register(t *x.Type) {
	registry.Register(t)
}

// Registry returns extension registry
func Registry() *x.Registry {
	return registry
}
