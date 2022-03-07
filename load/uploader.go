package load

import (
	"context"
	"github.com/viant/sqlx/option"
)

type Load func(ctx context.Context, any interface{}, options ...option.Option) (int, error)
