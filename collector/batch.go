package collector

import (
	"context"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/gds/fmap"
	"github.com/viant/rta/collector/config"
	"github.com/viant/rta/shared"
	tconfig "github.com/viant/tapper/config"
	"github.com/viant/tapper/log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type (
	Batch struct {
		ID string
		*tconfig.Stream
		Accumulator *Accumulator
		Started     time.Time
		logger      *log.Logger
		PendingURL  string
		sync.Mutex
		collecting        int32
		flushStarted      uint32
		pendingURLSymLink string
		streamURLSymLink  string
	}

	Accumulator struct {
		Map        map[interface{}]interface{}
		UseFastMap bool
		FastMap    *fmap.FastMap[any]
		size       uint32
		sync.RWMutex
	}
)

func (a *Accumulator) Len() int {
	return int(atomic.LoadUint32(&a.size))
}

func (a *Accumulator) Get(key interface{}) (interface{}, bool) {
	var data interface{}
	var ok bool
	if a.UseFastMap {
		scn := a.FastMap.SCN()
		data, ok = a.FastMap.Get(int64(key.(int)))
		next := a.FastMap.SCN()
		if scn != next {
			a.RWMutex.RLock()
			data, ok = a.FastMap.Get(int64(key.(int)))
			a.RWMutex.RUnlock()
		}

	} else {
		a.RWMutex.RLock()
		data, ok = a.Map[key]
		a.RWMutex.RUnlock()
	}

	return data, ok
}

func (a *Accumulator) Put(key, value interface{}) interface{} {
	a.RWMutex.Lock()
	defer a.RWMutex.Unlock()

	if a.UseFastMap {
		k := int64(key.(int))
		if prev, _ := a.FastMap.Get(k); prev != nil {
			return prev
		}
		a.FastMap.Put(int64(key.(int)), value)
		atomic.StoreUint32(&a.size, uint32(a.FastMap.Size()))
	} else {
		a.Map[key] = value
		atomic.StoreUint32(&a.size, uint32(len(a.Map)))
	}
	return value
}

func NewAccumulator(fastMap *FMapPool) *Accumulator {
	useFastMap := fastMap != nil
	var fMap *fmap.FastMap[any]
	if useFastMap {
		fMap = fastMap.Get()
	}
	ret := &Accumulator{UseFastMap: useFastMap, FastMap: fMap}
	if !useFastMap {
		ret.Map = make(map[interface{}]interface{}, 100)
	}
	return ret
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
			Stream:            &tconfig.Stream{}, // TODO check if nil is also correct
			Accumulator:       NewAccumulator(opts.pool),
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
		Accumulator:       NewAccumulator(opts.pool),
		Started:           time.Now(),
		logger:            logger,
		pendingURLSymLink: pendingURLSymLink,
		streamURLSymLink:  streamURLSymLink,
	}, nil
}
