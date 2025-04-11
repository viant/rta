package registry

// newFn defines the signature for factory functions.
type newFn func() interface{}

// NewFnRegistry maps names to factory functions.
type NewFnRegistry map[string]newFn

// Default registry instance.
var newFnRegistry = NewFnRegistry{}

// Register adds a factory function to the registry.
func (r NewFnRegistry) Register(name string, fn newFn) {
	r[name] = fn
}

// Lookup retrieves a factory function from the registry.
func (r NewFnRegistry) Lookup(name string) (newFn, bool) {
	fn, ok := r[name]
	return fn, ok
}

func LookupNewFn(name string) (newFn, bool) {
	return newFnRegistry.Lookup(name)
}
