package config

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/rta/config"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"io/ioutil"
)

type Config struct {
	Dest             string             `yaml:"Dest"`
	TransientDb      string             `yaml:"TransientDb"`
	JournalTable     string             `yaml:"JournalTable"`
	Connection       *config.Connection `yaml:"Connection"`
	UseCreateLikeDDL bool               `yaml:"UseCreateLikeDDL"`
	UseInsertAPI     bool
	suffix           Suffix
}

func (c *Config) TransientTable() string {
	if c.TransientDb == "" {
		return c.Dest
	}
	return c.TransientDb + "." + c.Dest
}

func (c *Config) Suffix() Suffix {
	if c.suffix == nil {
		return TimeBasedSuffix
	}
	return c.suffix
}

func (c *Config) SetSuffix(suffix Suffix) {
	c.suffix = suffix
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
