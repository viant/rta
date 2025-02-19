package config

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/rta/config"
	lconfig "github.com/viant/rta/load/config"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"io"
	"strings"
	"time"
)

const (
	defaultTimeoutSec   = 500
	defaultThinkTimeSec = 1
	LogPrefix           = "rta fsmerger -"
)

type (
	Config struct {
		Dest              string
		Connection        *config.Connection
		JournalTable      string
		JournalConnection *config.Connection
		Merge             *Merge
		TimeoutSec        int
		ThinkTimeSec      int
		Endpoint          *Endpoint
		Debug             bool
		TypeName          string
		DestPlaceholders  *DestPlaceholders
		Mode              string
		CreateDDL         string
		UseInsertAPI      bool
		BatchSize         int
		MainLoopDelayMs   int
		MergersRefreshMs  int
	}

	DestPlaceholders struct {
		Placeholders []string
		Connection   *config.Connection
		Query        string
	}

	Endpoint struct {
		Port int
	}
)

func (c *Config) Timeout() time.Duration {
	if c.TimeoutSec == 0 {
		c.TimeoutSec = defaultTimeoutSec
	}
	return time.Second * time.Duration(c.TimeoutSec)
}

func (c *Config) ThinkTime() time.Duration {
	if c.ThinkTimeSec == 0 {
		c.ThinkTimeSec = defaultThinkTimeSec
	}
	return time.Second * time.Duration(c.ThinkTimeSec)
}

type Merge struct {
	UniqueKeys    []string `yaml:"UniqueKeys"`
	AggregableSum []string `yaml:"AggregableSum"`
	AggregableMax []string `yaml:"AggregableMax"`
	Overridden    []string `yaml:"Overridden"`
	Others        []string `yaml:"Others"`
}

func (c *Config) Validate() error {
	prefix := fmt.Sprintf("%s config validation:", LogPrefix)
	required := map[string]string{
		"JournalTable":      c.JournalTable,
		"JournalConnection": fmt.Sprintf("%v", c.JournalConnection),
		"Driver":            c.JournalConnection.Driver,
		"DSN":               c.JournalConnection.Dsn,
	}

	for field, value := range required {
		if value == "" {
			return fmt.Errorf("%s %s was empty", prefix, field)
		}
	}

	if c.Mode != lconfig.Direct {
		return fmt.Errorf("%s unsupported loader's Mode: %s (only %s is supported)", prefix, c.Mode, lconfig.Direct)
	}
	if !c.UseInsertAPI {
		return fmt.Errorf("%s flag UseInsertAPI with value true is required", prefix)
	}

	if c.BatchSize == 0 {
		return fmt.Errorf("%s BatchSize is 0", prefix)
	}

	if c.Merge == nil {
		return fmt.Errorf("%s Merge was nil", prefix)
	}

	if c.DestPlaceholders != nil {
		hasConn := c.DestPlaceholders.Connection != nil
		hasQuery := c.DestPlaceholders.Query != ""

		if hasConn && !hasQuery {
			return fmt.Errorf("%s DestPlaceholders.Query was empty", prefix)
		}

		if !hasConn && hasQuery {
			return fmt.Errorf("%s DestPlaceholders.Connection was nil", prefix)
		}

		if hasConn && c.DestPlaceholders.Connection.Dsn == "" {
			return fmt.Errorf("%s DestPlaceholders.Connection.Dsn was empty", prefix)
		}

		if hasConn && c.DestPlaceholders.Connection.Driver == "" {
			return fmt.Errorf("%s DestPlaceholders.Connection.Driver was empty", prefix)
		}

		if c.DestPlaceholders.Placeholders == nil {
			return fmt.Errorf("%s DestPlaceholders.Placeholders was nil", prefix)
		}
	}

	return nil
}

func NewConfigFromURL(ctx context.Context, URL string) (cfg *Config, err error) {
	prefix := LogPrefix
	fs := afs.New()
	reader, err := fs.OpenURL(ctx, URL)
	if err != nil {
		return nil, fmt.Errorf("%s failed to get config: %v due to: %w", prefix, URL, err)
	}
	defer func() { err = errors.Join(err, reader.Close()) }()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("%s failed to load config: %v due to: %w", prefix, URL, err)
	}

	aMap := map[string]interface{}{}
	if err := yaml.Unmarshal(data, &aMap); err != nil {
		return nil, err
	}

	cfg = &Config{}
	err = toolbox.DefaultConverter.AssignConverted(cfg, aMap)
	if err != nil {
		return nil, fmt.Errorf("%s failed to convert config: %v due to: %w", prefix, URL, err)
	}
	return cfg, cfg.Validate()
}

func (c *Config) PrepareMergeFsConfig() *Config {
	var result = *c
	result.DestPlaceholders = nil // regular merger config does not need placeholders
	return &result
}

func (c *Config) ExpandConfig(name string, typeDef string) {
	c.JournalTable = c.expandTableName(c.JournalTable, name)
	c.Dest = c.expandTableName(c.Dest, name)
	c.CreateDDL = c.expandCreateDDL(c.CreateDDL, typeDef, c.Dest)
}

func (c *Config) expandTableName(template, tableName string) string {
	if template == "" || tableName == "" {
		return template
	}
	result := strings.ReplaceAll(template, "${Dest_lowercase}", strings.ToLower(tableName))
	return strings.ReplaceAll(result, "${Dest}", tableName)
}

func (c *Config) expandCreateDDL(template, typeDef, dest string) string {
	if template == "" {
		return template
	}

	result := strings.ReplaceAll(template, "${Struct}", typeDef)
	return strings.ReplaceAll(result, "${Dest}", dest)
}
