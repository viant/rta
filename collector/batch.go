package collector

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/rta/collector/config"
	"github.com/viant/rta/shared"
	tconfig "github.com/viant/tapper/config"
	"github.com/viant/tapper/log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Batch struct {
	ID string
	*tconfig.Stream
	Accumulator *Accumulator
	Started     time.Time
	logger      *log.Logger
	PendingURL  string
	flushAfter  *time.Time
	sync.Mutex
	collecting        int32
	flushStarted      uint32
	pendingURLSymLink string
	streamURLSymLink  string
}

func (b *Batch) HasPendingTransaction() bool {
	return atomic.LoadInt32(&b.collecting) > 0
}

func (b *Batch) IsReadyForFlush(ts *time.Time) bool {
	if b.HasPendingTransaction() {
		return false
	}
	return ts.After(*b.flushAfter)
}

func (b *Batch) ensureNoPending() {
	for i := 0; i < 1200; i++ { //extra safety check
		if !b.HasPendingTransaction() {
			return
		}
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("ensureNoPending: %v\n", i)
	}
	fmt.Printf("pending transaction still exists\n")
}

func (b *Batch) removePendingFile(ctx context.Context, fs afs.Service) error {
	if b.PendingURL == "" {
		return nil
	}
	if ok, _ := fs.Exists(ctx, b.PendingURL); ok {
		if err := fs.Delete(ctx, b.PendingURL); err != nil {
			return err
		}
	}

	if b.pendingURLSymLink != "" {
		return fs.Delete(ctx, b.pendingURLSymLink)
	}
	return nil
}

func (b *Batch) removeDataFile(ctx context.Context, fs afs.Service) error {
	if b.Stream.URL == "" {
		return nil
	}
	if ok, _ := fs.Exists(ctx, b.Stream.URL); ok {
		if err := fs.Delete(context.Background(), b.Stream.URL); err != nil {
			return err
		}
	}

	if b.streamURLSymLink != "" {
		return fs.Delete(context.Background(), b.streamURLSymLink)
	}
	return nil
}

func (b *Batch) IsActive(batch *config.Batch) bool {
	if atomic.LoadUint32(&b.flushStarted) == 1 {
		return false
	}
	if b.Accumulator.Len() >= batch.MaxElements {
		return false
	}
	return time.Now().Sub(b.Started) < batch.MaxDuration()
}

func NewBatch(stream *tconfig.Stream, disabled bool, fs afs.Service, options ...Option) (*Batch, error) {
	UUID := uuid.New()

	var URL string
	var pendingURLSymLink string
	var streamURLSymLink string
	var pendingURL string

	opts := NewOptions(options...)
	symLinkStreamURLTrg := opts.GetStreamURLSymLinkTrg()
	if disabled {
		return &Batch{
			PendingURL:        pendingURL,
			ID:                UUID.String(),
			Stream:            &tconfig.Stream{},
			Accumulator:       NewAccumulator(opts.fMapPool, opts.mapPool, opts.shardAccPool),
			Started:           time.Now(),
			logger:            nil,
			pendingURLSymLink: pendingURLSymLink,
			streamURLSymLink:  streamURLSymLink,
		}, nil
	}

	switch symLinkStreamURLTrg {
	case "":
		URL = stream.URL
	default:
		URL = symLinkStreamURLTrg
	}

	URL = URL + "_" + UUID.String()
	pendingURL = URL + shared.PendingSuffix

	batchSteam := &tconfig.Stream{
		FlushMod: stream.FlushMod,
		Codec:    stream.Codec,
		URL:      URL,
	}

	if symLinkStreamURLTrg != "" {
		streamURLSymLink = stream.URL + "_" + UUID.String()
		err := os.Symlink(url.Path(URL), url.Path(streamURLSymLink))
		if err != nil {
			return nil, err
		}

		pendingURLSymLink = stream.URL + "_" + UUID.String() + shared.PendingSuffix
		err = os.Symlink(url.Path(pendingURL), url.Path(pendingURLSymLink))
		if err != nil {
			return nil, err
		}
	}

	if err := fs.Create(context.Background(), pendingURL, file.DefaultFileOsMode, false); err != nil {
		return nil, err
	}
	logger, err := log.New(batchSteam, "", fs)
	if err != nil {
		return nil, err
	}
	return &Batch{
		PendingURL:        pendingURL,
		ID:                UUID.String(),
		Stream:            batchSteam,
		Accumulator:       NewAccumulator(opts.fMapPool, opts.mapPool, opts.shardAccPool),
		Started:           time.Now(),
		logger:            logger,
		pendingURLSymLink: pendingURLSymLink,
		streamURLSymLink:  streamURLSymLink,
	}, nil
}
