package collector

import (
	"context"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/rta/collector/config"
	"github.com/viant/rta/shared"
	tconfig "github.com/viant/tapper/config"
	"github.com/viant/tapper/log"
	"github.com/viant/toolbox"
	"os"
	"sync"
	"time"
)

type (
	Batch struct {
		ID string
		*tconfig.Stream
		Accumulator *Accumulator
		Started     time.Time
		Count       int32
		logger      *log.Logger
		PendingURL  string
		sync.Mutex
		flushed           uint32
		pendingURLSymLink string
		streamURLSymLink  string
	}

	Accumulator struct {
		Map map[interface{}]interface{}
		sync.RWMutex
	}
)

func (a *Accumulator) Len() int {
	a.RWMutex.RLock()
	result := len(a.Map)
	a.RWMutex.RUnlock()
	return result
}

func (a *Accumulator) Get(key interface{}) (interface{}, bool) {
	a.RWMutex.RLock()
	data, ok := a.Map[key]
	a.RWMutex.RUnlock()
	return data, ok
}

func (a *Accumulator) Put(key, value interface{}) {
	a.RWMutex.Lock()
	a.Map[key] = value
	a.RWMutex.Unlock()
}

func NewAccumulator() *Accumulator {
	return &Accumulator{Map: map[interface{}]interface{}{}}
}

func (b Batch) IsActive(batch *config.Batch) bool {
	return toolbox.AsInt(b.Accumulator.Len()) < batch.MaxElements && time.Now().Sub(b.Started) < batch.MaxDuration()
}

func NewBatch(stream *tconfig.Stream, fs afs.Service, options ...Option) (*Batch, error) {

	UUID := uuid.New()

	var URL string
	var pendingURLSymLink string
	var streamURLSymLink string

	symLinkStreamURLTrg := NewOptions(options...).GetStreamURLSymLinkTrg()
	switch symLinkStreamURLTrg {
	case "":
		URL = stream.URL
	default:
		URL = symLinkStreamURLTrg
	}

	URL = URL + "_" + UUID.String()
	pendingURL := URL + shared.PendingSuffix

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
		Accumulator:       NewAccumulator(),
		Started:           time.Now(),
		Count:             0,
		logger:            logger,
		pendingURLSymLink: pendingURLSymLink,
		streamURLSymLink:  streamURLSymLink,
	}, nil
}
