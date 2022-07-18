package config

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/rta/config"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"time"
)

type (
	Config struct {
		Dest           string `yaml:"Dest"`
		JournalTable   string `yaml:"JournalTable"`
		Merge          *Merge `yaml:"Merge"`
		DeleteDelaySec int
		Connection     *config.Connection `yaml:"Connection"`
		TimeoutSec     int
		ThinkTimeSec   int
		Endpoint       *Endpoint
	}

	Endpoint struct {
		Port int
	}
)

func (c *Config) DeleteDelay() time.Duration {
	if c.DeleteDelaySec == 0 {
		return time.Minute
	}
	return time.Duration(c.DeleteDelaySec) * time.Second
}

func (c *Config) Timeout() time.Duration {
	if c.TimeoutSec == 0 {
		c.TimeoutSec = 500
	}
	return time.Second * time.Duration(c.TimeoutSec)
}

func (c *Config) ThinkTime() time.Duration {
	if c.ThinkTimeSec == 0 {
		c.ThinkTimeSec = 1
	}
	return time.Second * time.Duration(c.ThinkTimeSec)
}

type Merge struct {
	UniqueKeys    []string `yaml:"UniqueKeys"`
	AggregableSum []string `yaml:"AggregableSum"`
	AggregableMax []string `yaml:"AggregableMax"`
	Others        []string `yaml:"Others"`
}

func (c *Config) Validate() error {
	if c.Dest == "" {
		return errors.Errorf("Dest was empty")
	}
	if c.JournalTable == "" {
		return errors.Errorf("JournalTable was empty")
	}
	if c.Connection == nil {
		return errors.Errorf("Connection was empty")
	}
	if c.Connection.Driver == "" {
		return errors.Errorf("Driver was empty")
	}
	if c.Connection.Dsn == "" {
		return errors.Errorf("Dsn was empty")
	}
	if c.Merge == nil {
		return errors.Errorf("Merge was nil")
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
