package loader

type (
	Options struct {
		instanceId string
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

func (o *Options) GetInstanceId() string {
	return o.instanceId
}
