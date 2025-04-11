package registry

type keyFn func(record interface{}) interface{}
type KeyFnRegistry map[string]keyFn

var keyFnRegistry KeyFnRegistry = make(KeyFnRegistry)

func (r KeyFnRegistry) Register(name string, fn keyFn) {
	keyFnRegistry[name] = fn
}

func (r KeyFnRegistry) lookUp(name string) (keyFn, bool) {
	v, ok := keyFnRegistry[name]
	return v, ok
}

func LookupKeyFn(name string) (keyFn, bool) {
	return keyFnRegistry.lookUp(name)
}
