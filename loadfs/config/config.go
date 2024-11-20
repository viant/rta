package config

import (
	"fmt"
	"github.com/viant/rta/shared"
)

// Config represents a file system loader configuration
type Config struct {
	URL            string
	FlushMod       int
	StreamUpload   bool
	MaxMessageSize int
	Concurrency    int
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("loader config validation: URL was empty")
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
