package collector

type (
	Options struct {
		streamURLSymLinkTrg string
		instanceId          string
		pool                *FMapPool
		keyPtrFn            func(record interface{}, ptr *interface{})
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

func WithFastMapPool(pool *FMapPool) Option {
	return func(o *Options) {
		o.pool = pool
	}
}

func WithKeyPointerFunc(fn func(record interface{}, ptr *interface{})) Option {
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
