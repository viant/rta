package mergefs

import (
	"context"
	"github.com/viant/rta/mergefs/config"
	"runtime/debug"
)

func RunApp(configURL string) error {
	cfg, err := config.NewConfigFromURL(context.Background(), configURL)
	if err != nil {
		return err
	}
	merger, err := New(cfg)
	if err != nil {
		return err
	}

	if cfg.GCPercent > 0 && cfg.GCPercent < 100 {
		debug.SetGCPercent(cfg.GCPercent)
	}

	merger.MergeInBackground()
	return nil
}
