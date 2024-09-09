package config

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/rta/load/config"
	"github.com/viant/rta/shared"
	tconfig "github.com/viant/tapper/config"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"time"
)

type Config struct {
	ID                  string
	Loader              *config.Config
	Stream              *tconfig.Stream
	MaxMessageSize      int
	Concurrency         int
	Batch               *Batch
	Retry               *Retry
	StreamDisabled      bool
	LoadDelayMaxMs      int
	LoadDelaySeedPart   int
	LoadDelayEveryNExec int
	LoadDelayOnlyOnce   bool
}

func (c *Config) IsStreamEnabled() bool {
	return !c.StreamDisabled
}

type Retry struct {
	EveryInSec int
	Max        int
}

type Batch struct {
	MaxElements   int
	maxDuration   time.Duration
	MaxDurationMs int
}

func (b *Batch) MaxDuration() time.Duration {
	if b.maxDuration != 0 {
		return b.maxDuration
	}
	b.maxDuration = time.Millisecond * time.Duration(b.MaxDurationMs)
	return b.maxDuration

}

func (c *Config) Validate() error {
	if c.Loader.Dest == "" {
		return errors.Errorf("Dest was empty")
	}
	if c.Loader.JournalTable == "" {
		return errors.Errorf("JournalTable was empty")
	}
	if c.Loader.Connection == nil {
		return errors.Errorf("Connection was empty")
	}
	if c.Loader.Connection.Driver == "" {
		return errors.Errorf("Driver was empty")
	}
	if c.Loader.Connection.Dsn == "" {
		return errors.Errorf("Dsn was empty")
	}

	if c.MaxMessageSize < shared.DefaultMaxMessageSize {
		c.MaxMessageSize = shared.DefaultMaxMessageSize
	}

	if c.Concurrency <= 0 {
		c.Concurrency = shared.DefaultConcurrency
	}

	if c.Batch == nil {
		return errors.Errorf("Batch was empty")
	}
	if c.Batch.MaxDurationMs <= 0 {
		return errors.Errorf("Batch MaxDurationMs was 0")
	}
	if c.Batch.MaxElements <= 0 {
		return errors.Errorf("Batch MaxElements was 0")
	}

	return nil
}

func NewConfigFromURL(ctx context.Context, URL string) (*Config, error) {
	fs := afs.New()
	reader, err := fs.OpenURL(ctx, URL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get config: %v", URL)
	}
	defer reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load config: %v", URL)
	}
	transient := map[string]interface{}{}
	if err := yaml.Unmarshal(data, &transient); err != nil {
		return nil, err
	}
	aMap := map[string]interface{}{}
	yaml.Unmarshal(data, &aMap)
	cfg := &Config{}
	err = toolbox.DefaultConverter.AssignConverted(cfg, aMap)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to convert config: %v", URL)
	}
	return cfg, cfg.Validate()
}
