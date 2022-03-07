package collector

import "context"

type Loader interface {
	Load(ctx context.Context, data interface{}, batchID string) error
}
