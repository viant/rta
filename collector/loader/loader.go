package loader

import "context"

type Loader interface {
	Load(ctx context.Context, data interface{}, batchID string, options ...Option) error
}

type FunctionLoader struct {
	LoadFn func(ctx context.Context, data interface{}, batchID string, options ...Option) error
}

func (m *FunctionLoader) Load(ctx context.Context, data interface{}, batchID string, options ...Option) error {
	return m.LoadFn(ctx, data, batchID, options...)
}
