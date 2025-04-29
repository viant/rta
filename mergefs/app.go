package mergefs

import (
	"context"
	"github.com/viant/rta/mergefs/config"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
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

	if cfg.GCPercent > 0 && cfg.GCPercent <= 100 {
		debug.SetGCPercent(cfg.GCPercent)
	}

	log.Printf("started rta-mergefs endpoint: %v\n", cfg.Endpoint.Port)

	ctxGlobal, cancelGlobal := context.WithCancel(context.Background())
	defer cancelGlobal()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go merger.shutDownOnInterrupt(sigCh, cancelGlobal)

	merger.MergeInBackground(ctxGlobal)
	return nil
}
