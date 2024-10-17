package collector

import (
	"context"
	"fmt"
	"github.com/dolthub/swiss"
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

type (
	Batch struct {
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

	Accumulator struct {
		Map        map[interface{}]interface{}
		UseFastMap bool
		FastMap    *swiss.Map[any, any]
		size       uint32
		sync.RWMutex
	}
)

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

func (a *Accumulator) Len() int {
	return int(atomic.LoadUint32(&a.size))
}

func (a *Accumulator) GetOrCreate(key interface{}, get func() interface{}) (interface{}, bool) {
	var data interface{}
	var ok bool
	if a.UseFastMap {
		a.RWMutex.RLock()
		data, ok = a.FastMap.Get(key)
		a.RWMutex.RUnlock()
		if !ok {
			a.RWMutex.Lock()
			data, ok = a.FastMap.Get(key)
			if !ok {
				data = get()
				ok = true
				a.FastMap.Put(key, data)
				atomic.StoreUint32(&a.size, uint32(a.FastMap.Count()))
			}
			a.RWMutex.Unlock()
		}
	} else {
		a.RWMutex.RLock()
		data, ok = a.Map[key]
		a.RWMutex.RUnlock()
		if !ok {
			a.RWMutex.Lock()
			data, ok = a.Map[key]
			if !ok {
				data = get()
				a.Map[key] = data
				ok = true
				atomic.StoreUint32(&a.size, uint32(len(a.Map)))
			}
			a.RWMutex.Unlock()
		}
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
		atomic.StoreUint32(&a.size, uint32(a.FastMap.Count()))
	} else {
		a.Map[key] = value
		atomic.StoreUint32(&a.size, uint32(len(a.Map)))
	}
	return value
}

func NewAccumulator(fastMap *FMapPool) *Accumulator {
	useFastMap := fastMap != nil
	var fMap *swiss.Map[any, any]
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
		PendingURL: pendingURL,

		ID:                UUID.String(),
		Stream:            batchSteam,
		Accumulator:       NewAccumulator(opts.pool),
		Started:           time.Now(),
		logger:            logger,
		pendingURLSymLink: pendingURLSymLink,
		streamURLSymLink:  streamURLSymLink,
	}, nil
}
