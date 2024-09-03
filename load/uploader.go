package load

import (
	"context"
	"database/sql"
)

type (
	options struct {
		db *sql.DB
	}
	Option func(o *options)
)

func WithDb(db *sql.DB) Option {
	return func(o *options) {
		o.db = db
	}
}

func newOptions(opts []Option) *options {
	ret := &options{}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

type Load func(ctx context.Context, any interface{}, options ...Option) (int, error)
