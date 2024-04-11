package collector

type (
	Options struct {
		streamURLSymLinkTrg string
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

func WithStreamURLSymLinkTrg(streamURLSymLinkTrg string) Option {
	return func(o *Options) {
		o.streamURLSymLinkTrg = streamURLSymLinkTrg
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
