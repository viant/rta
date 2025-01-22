package config

import (
	"fmt"
	"github.com/viant/rta/config"
	"github.com/viant/rta/shared"
)

// Config represents a file system loader configuration
type Config struct {
	URL            string
	FlushMod       int
	StreamUpload   bool
	MaxMessageSize int
	Concurrency    int
	ConnectionJn   *config.Connection
	JournalTable   string
	CreateJnDDL    string
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("loader config validation: URL was empty")
	}
	if c.JournalTable == "" && c.ConnectionJn != nil {
		return fmt.Errorf("loader config validation: JournalTable was empty")
	}
	if c.JournalTable != "" && c.ConnectionJn == nil {
		return fmt.Errorf("loader config validation: ConnectionJn was nil")
	}

	return nil
}

// Init initializes the configuration
func (c *Config) Init() error {
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = shared.DefaultMaxMessageSize
	}

	if c.Concurrency <= 0 {
		c.Concurrency = shared.DefaultConcurrency
	}
	return nil
}

func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	ret := *c
	return &ret
}
