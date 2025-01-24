package registry

import (
	"github.com/viant/x"
	"strings"
)

const pkg = "github.com/viant/rta/mergefs/registry"

var registry = x.NewRegistry()

// Registry returns extension registry
func Registry() *x.Registry {
	return registry
}

// LookUp returns a type by name
func LookUp(r *x.Registry, name string) *x.Type {
	if idx := strings.Index(name, "."); idx == -1 {
		name = pkg + "." + name
	}
	return r.Lookup(name)
}
