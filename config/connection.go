package config

import (
	"context"
	"database/sql"

	"github.com/viant/scy"
)

type Connection struct {
	Driver              string `yaml:"Driver"`
	Dsn                 string `yaml:"Dsn"`
	MetaSessionCacheKey string `yaml:"MetaSessionCacheKey"`
	MaxOpenConns        int
	MaxIdleConns        int
	MaxIdleTimeMs       int
	MaxLifetimeMs       int
	Secret              *scy.Resource
}

func (c *Connection) OpenDB(ctx context.Context) (*sql.DB, string, error) {
	metaSessionCacheKey := c.MetaSessionCacheKey
	dsn := c.Dsn
	if c.Secret != nil {
		secrets := scy.New()
		secret, err := secrets.Load(ctx, c.Secret)
		if err != nil {
			return nil, "", err
		}
		dsn = secret.Expand(dsn)
		if metaSessionCacheKey != "" {
			metaSessionCacheKey = secret.Expand(metaSessionCacheKey)
		}
	}
	db, err := sql.Open(c.Driver, dsn)
	return db, metaSessionCacheKey, err
}
