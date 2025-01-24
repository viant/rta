package mergefs

import (
	"context"
	"github.com/viant/rta/mergefs/config"
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
	merger.MergeInBackground()
	return nil
}
