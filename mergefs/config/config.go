package config

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/afs"
	cconfig "github.com/viant/rta/collector/config"
	"github.com/viant/rta/config"
	lconfig "github.com/viant/rta/load/config"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"io"
	"strings"
	"time"
)

const (
	defaultTimeoutSec           = 500
	defaultThinkTimeSec         = 1
	defaultMergersRefreshMs     = 1000
	LogPrefix                   = "rta fsmerger -"
	minCollectorBatchElements   = 2000000000
	minCollectorBatchDurationMs = 2000000000
)

type (
	Config struct {
		Dest               string
		Connection         *config.Connection
		JournalTable       string
		JournalConnection  *config.Connection
		Merge              *Merge
		TimeoutSec         int
		ThinkTimeSec       int
		ThinkTimeJournalMs int
		Endpoint           *Endpoint
		Debug              bool
		TypeName           string
		DestPlaceholders   *DestPlaceholders
		Mode               string
		CreateDDL          string
		UseInsertAPI       bool
		BatchSize          int
		MainLoopDelayMs    int
		MergersRefreshMs   int
		Collector          *cconfig.Config
		MaxJournalsInChunk int
		ScannerBufferMB    int //use in case you see bufio.Scanner: token too long
		GCPercent          int
		ForceQuitTimeSec   int
	}

	DestPlaceholders struct {
		Placeholders []string
		Connection   *config.Connection
		Query        string
	}

	Endpoint struct {
		Port int
	}

	Merge struct {
		UniqueKeys    []string `yaml:"UniqueKeys"`
		AggregableSum []string `yaml:"AggregableSum"`
		AggregableMax []string `yaml:"AggregableMax"`
		Overridden    []string `yaml:"Overridden"`
		Others        []string `yaml:"Others"`
	}
)

func (c *Config) init() error {
	if c.MergersRefreshMs == 0 {
		c.MergersRefreshMs = defaultMergersRefreshMs
	}

	if c.TimeoutSec == 0 {
		c.TimeoutSec = defaultTimeoutSec
	}

	if c.TimeoutSec == 0 {
		c.TimeoutSec = defaultTimeoutSec
	}

	if c.ThinkTimeSec == 0 {
		c.ThinkTimeSec = defaultThinkTimeSec
	}

	if c.GCPercent < 1 || c.GCPercent > 100 {
		c.GCPercent = 100
	}

	if c.ForceQuitTimeSec < 1 {
		c.ForceQuitTimeSec = 180
	}

	return nil
}

func (c *Config) Timeout() time.Duration {
	return time.Second * time.Duration(c.TimeoutSec)
}

func (c *Config) ThinkTime() time.Duration {
	return time.Second * time.Duration(c.ThinkTimeSec)
}

func (c *Config) ThinkTimeJournal() time.Duration {
	return time.Millisecond * time.Duration(c.ThinkTimeJournalMs)
}

func (c *Config) Validate() error {
	if c.Collector == nil {
		return c.validate()
	} else {
		return c.validateWithCollector()
	}
}

func (c *Config) validateWithCollector() error {
	prefix := getPrefix()
	err := c.validateRequired(prefix)
	if err != nil {
		return err
	}

	err = c.validatePlaceholders(prefix)
	if err != nil {
		return err
	}

	err = c.validateCollector(prefix)
	if err != nil {
		return err
	}

	c.makeWarnings(prefix)

	return nil
}

func (c *Config) makeWarnings(prefix string) {
	if c.Dest != "" {
		fmt.Printf("%s warning: Dest is ignored, use collector.Loader.Dest instead\n", prefix)
	}

	if c.Connection != nil {
		fmt.Printf("%s warning: Connection is ignored, use collector.Loader.Connection instead\n", prefix)
	}

	if c.Merge != nil {
		fmt.Printf("%s warning: Merge is ignored, use collector.Loader.Merge instead\n", prefix)
	}

	if c.Mode != "" {
		fmt.Printf("%s warning: Mode is ignored, use collector.Loader.Mode instead\n", prefix)
	}

	if c.UseInsertAPI {
		fmt.Printf("%s warning: UseInsertAPI is ignored, use collector.Loader.UseInsertAPI instead\n", prefix)
	}

	if c.BatchSize != 0 {
		fmt.Printf("%s warning: BatchSize is ignored, use collector.Loader.BatchSize instead\n", prefix)
	}

	if c.CreateDDL != "" {
		fmt.Printf("%s warning: CreateDDL is ignored, use collector.Loader.CreateDDL instead\n", prefix)
	}
}

func (c *Config) validateCollector(prefix string) error {
	if err := c.Collector.Validate(); err != nil {
		return fmt.Errorf("%s %w", prefix, err)
	}
	if c.Collector.Mode != cconfig.ManualMode {
		return fmt.Errorf("%s unsupported collector's Mode: %s (only %s is supported)", prefix, c.Collector.Mode, cconfig.ManualMode)
	}

	if c.Collector.Loader == nil {
		return fmt.Errorf("%s %s was empty", prefix, "c.Collector.Loader")
	}

	if err := c.Collector.Loader.Validate(); err != nil {
		return fmt.Errorf("%s %w", prefix, err)
	}

	if c.MaxJournalsInChunk < 1 {
		return fmt.Errorf("%s %s was lower than 1", prefix, "MaxJournalsInChunk")
	}

	if c.Collector.Loader.BatchSize < 1 {
		return fmt.Errorf("%s %s was lower than 1", prefix, "c.Collector.Loader.BatchSize")
	}

	if c.Collector.Batch == nil {
		return fmt.Errorf("%s %s was nil", prefix, "c.Collector.Batch")
	}

	//the number of elements has to be impossible to achieve
	if c.Collector.Batch.MaxElements < minCollectorBatchElements {
		return fmt.Errorf("%s %s was lower than %d (the number of elements has to be impossible to achieve)", prefix, "c.Collector.Batch.MaxElements", minCollectorBatchElements)
	}

	//the duration has to be impossible to exceed
	if c.Collector.Batch.MaxDurationMs < minCollectorBatchDurationMs {
		return fmt.Errorf("%s %s was lower than %d (the duration has to be impossible to exceed)", prefix, "c.Collector.Batch.MaxDurationMs", minCollectorBatchDurationMs)
	}

	return nil
}

func (c *Config) validatePlaceholders(prefix string) error {
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

func (c *Config) validateRequired(prefix string) error {
	required := map[string]string{
		"JournalTable":      c.JournalTable,
		"JournalConnection": fmt.Sprintf("%v", c.JournalConnection),
	}

	for field, value := range required {
		if value == "" {
			return fmt.Errorf("%s %s was empty", prefix, field)
		}
	}

	required2 := map[string]string{
		"Driver": c.JournalConnection.Driver,
		"DSN":    c.JournalConnection.Dsn,
	}

	if c.Endpoint == nil {
		return fmt.Errorf("%s %s was nil", prefix, "Endpoint")
	}

	if c.Endpoint.Port == 0 {
		return fmt.Errorf("%s %s equals 0", prefix, "Endpoint.Port")
	}

	for field, value := range required2 {
		if value == "" {
			return fmt.Errorf("%s %s was empty", prefix, field)
		}
	}

	if c.GCPercent < 1 || c.GCPercent > 100 {
		return fmt.Errorf("%s GCPercent was lower than 1 or higher than 100", prefix)
	}

	if c.ScannerBufferMB < 0 {
		return fmt.Errorf("%s ScannerBufferMB was lower than 0", prefix)
	}

	return nil
}

func (c *Config) validate() error {
	prefix := getPrefix()
	err := c.validateRequired(prefix)
	if err != nil {
		return err
	}

	err = c.validatePlaceholders(prefix)
	if err != nil {
		return err
	}

	err = c.validateLoaderRelatedParams(prefix)
	if err != nil {
		return err
	}

	return nil
}

func (c *Config) validateLoaderRelatedParams(prefix string) error {
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

	err = cfg.init()
	if err != nil {
		return nil, err
	}

	return cfg, cfg.Validate()
}

func (c *Config) PrepareMergeFsConfig() *Config {
	var result = *c
	result.DestPlaceholders = nil // regular merger config does not need placeholders
	if c.Collector != nil {
		result.Collector = c.Collector.Clone()
		result.Dest = c.Collector.Loader.Dest
	}
	return &result
}

func (c *Config) ExpandConfig(name string, typeDef string) {
	c.JournalTable = c.expandTableName(c.JournalTable, name)
	c.Dest = c.expandTableName(c.Dest, name)
	c.CreateDDL = c.expandCreateDDL(c.CreateDDL, typeDef, c.Dest)

	if c.Collector != nil {
		c.Collector.Loader.Dest = c.expandTableName(c.Collector.Loader.Dest, name)
		c.Collector.Loader.CreateDDL = c.expandCreateDDL(c.Collector.Loader.CreateDDL, typeDef, c.Collector.Loader.Dest)
	}
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

func getPrefix() string {
	return fmt.Sprintf("%s config validation:", LogPrefix)
}
