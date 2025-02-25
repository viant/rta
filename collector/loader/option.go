package loader

type (
	Options struct {
		instanceId string
		category   string
		convertFn  func(record interface{}) (interface{}, error)
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

func WithCategory(category string) Option {
	return func(o *Options) {
		o.category = category
	}
}

func WithConvertFn(fn func(record interface{}) (interface{}, error)) Option {
	return func(o *Options) {
		o.convertFn = fn
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

func (o *Options) Category() string {
	return o.category
}

func (o *Options) GetConvertFunc() func(record interface{}) (interface{}, error) {
	return o.convertFn
}
