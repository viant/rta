package collector

import loader2 "github.com/viant/rta/collector/loader"

type (
	Options struct {
		streamURLSymLinkTrg string
		instanceId          string
		fMapPool            *FMapPool
		mapPool             *MapPool
		keyPtrFn            func(record interface{}, ptr interface{})
		fsLoader            loader2.Loader
		category            string
	}

	Option func(o *Options)
)

func NewOptions(options ...Option) *Options {
	ret := &Options{}
	for _, item := range options {
		item(ret)
	}
	return ret
}

func WithFastMapPool(fMapPool *FMapPool) Option {
	return func(o *Options) {
		o.fMapPool = fMapPool
	}
}

func WithMapPool(mapPool *MapPool) Option {
	return func(o *Options) {
		o.mapPool = mapPool
	}
}

func WithKeyPointerFunc(fn func(record interface{}, ptr interface{})) Option {
	return func(o *Options) {
		o.keyPtrFn = fn
	}
}

func WithStreamURLSymLinkTrg(streamURLSymLinkTrg string) Option {
	return func(o *Options) {
		o.streamURLSymLinkTrg = streamURLSymLinkTrg
	}
}

func WithInstanceId(instanceId string) Option {
	return func(o *Options) {
		o.instanceId = instanceId
	}
}

func WithCategory(category string) Option {
	return func(o *Options) {
		o.category = category
	}
}

func WithFsLoader(loader loader2.Loader) Option {
	return func(o *Options) {
		o.fsLoader = loader
	}
}

func (o *Options) Apply(opts ...Option) {
	if len(opts) == 0 {
		return
	}
	for _, opt := range opts {
		opt(o)
	}
}

func (o *Options) GetStreamURLSymLinkTrg() string {
	return o.streamURLSymLinkTrg
}

func (o *Options) GetInstanceId() string {
	return o.instanceId
}

func (o *Options) Category() string {
	return o.category
}

func (o *Options) FsLoader() loader2.Loader {
	return o.fsLoader
}
