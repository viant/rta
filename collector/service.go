package collector

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/gmetric"
	"github.com/viant/gmetric/provider"
	"github.com/viant/gmetric/stat"
	"github.com/viant/rta/collector/config"
	loader2 "github.com/viant/rta/collector/loader"
	"github.com/viant/rta/shared"
	"github.com/viant/tapper/io"
	"github.com/viant/tapper/io/encoder"
	"github.com/viant/tapper/msg"
	tjson "github.com/viant/tapper/msg/json"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	trashDirPrefix    = "CHECK_ME_BEFORE_DELETE_"
	trashDirSuffixFmt = "20060102_150405"
)

type Service struct {
	config      *config.Config
	fs          afs.Service
	newRecord   func() interface{}
	keyFn       func(record interface{}) interface{}
	reducerFn   func(accumulator, source interface{})
	mapperFn    func(acc *Accumulator) interface{}
	loader      loader2.Loader
	batch       *Batch
	mux         sync.Mutex
	encProvider *encoder.Provider
	*msg.Provider
	flushCounter  *gmetric.Operation
	retryCounter  *gmetric.Operation
	loadMux       sync.Mutex
	pendingRetry  int32
	notifyPending chan bool
	metrics       *gmetric.Service
	options       []Option
}

func (s *Service) NotifyWatcher() {
	select {
	case s.notifyPending <- true:
	default:
	}
}

func (s *Service) watchInBackground() {
	sleepTime := time.Second
	if s.config.Batch != nil {
		sleepTime = s.config.Batch.MaxDuration()
	}

	for <-s.notifyPending {
		b, err := s.getBatch()

		if err != nil {
			log.Println(err)
		}

		if b != nil && atomic.LoadUint32(&b.flushed) == 0 && b.Accumulator.Len() > 0 {
			time.Sleep(sleepTime)
			s.NotifyWatcher()
		}
	}
}

func (s *Service) RetryFailed(ctx context.Context, onStartUp bool) error {
	if !atomic.CompareAndSwapInt32(&s.pendingRetry, 0, 1) {
		return nil
	}

	streamURLSymLinkTrg := NewOptions(s.options...).GetStreamURLSymLinkTrg()
	streamURLSymLinkTrgBase := ""
	if streamURLSymLinkTrg != "" {
		streamURLSymLinkTrgBase, _ = url.Split(streamURLSymLinkTrg, file.Scheme)
	}

	toRetry, err := s.loadFailedBatches(ctx, onStartUp, streamURLSymLinkTrgBase)
	if len(toRetry) == 0 {
		atomic.StoreInt32(&s.pendingRetry, 0)
		return err
	}

	if err != nil {
		atomic.StoreInt32(&s.pendingRetry, 0)
		return err
	}
	errors := shared.Errors{}
	wg := sync.WaitGroup{}
	wg.Add(len(toRetry))

	concurrency := s.config.Concurrency
	rateLimiter := make(chan bool, concurrency)
	for _, URL := range toRetry {
		go func(URL string) {
			defer wg.Done()
			rateLimiter <- true
			if err := s.replayBatch(ctx, URL, streamURLSymLinkTrgBase); err != nil {
				errors.Add(err)
			}
			<-rateLimiter
		}(URL)
	}
	wg.Wait()
	atomic.StoreInt32(&s.pendingRetry, 0)
	return errors.First()
}

func (s *Service) loadFailedBatches(ctx context.Context, onStartUp bool, streamURLSymLinkBase string) ([]string, error) {
	URL := url.Normalize(s.config.Stream.URL, file.Scheme)
	parentURL, _ := url.Split(URL, file.Scheme)
	objects, err := s.fs.List(ctx, parentURL)
	if err != nil {
		return nil, err
	}
	toProcess := make(map[string]bool)
	pending := make(map[string]bool)
	for i := range objects {
		candidate := objects[i]
		if candidate.IsDir() {
			continue
		}
		if strings.HasSuffix(candidate.URL(), shared.PendingSuffix) {
			if onStartUp {
				_ = s.fs.Delete(ctx, candidate.URL())
				if streamURLSymLinkBase != "" {
					aFile := url.Join(streamURLSymLinkBase, candidate.Name())
					_ = s.fs.Delete(ctx, aFile)
				}
				continue
			}
			pending[candidate.URL()] = true
			continue
		}
		toProcess[candidate.URL()] = true
	}

	var toRetry = make([]string, 0, len(toProcess))
	for candidate := range toProcess {
		if pending[candidate+shared.PendingSuffix] {
			continue
		}
		toRetry = append(toRetry, candidate)
	}
	return toRetry, nil
}

func (s *Service) getBatch() (*Batch, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if batch := s.batch; batch != nil {
		b, err := s.flushIfNeed(batch)
		if b != nil {
			return b, err
		}
	}
	batch, err := NewBatch(s.config.Stream, s.fs, s.options...)
	if err != nil {
		return nil, err
	}
	s.batch = batch
	return batch, nil
}

func (s *Service) flushIfNeed(batch *Batch) (*Batch, error) {
	if batch.IsActive(s.config.Batch) {
		return batch, nil
	}
	go func() {
		if err := s.FlushInBackground(batch); err != nil {
			log.Println(err)
		}
	}()
	return nil, nil
}

func (s *Service) Collect(record interface{}) error {
	batch, err := s.getBatch()
	if err != nil {
		return err
	}
	data := batch.Accumulator
	s.reduce(data, record)
	message := s.Provider.NewMessage()

	enc, ok := record.(io.Encoder)
	if !ok {
		provider, err := s.encoderProvider(record)
		if err != nil {
			return err
		}
		enc = provider.New(record)
	}

	enc.Encode(message)
	if err = batch.logger.Log(message); err != nil {
		return err
	}
	message.Free()
	_, err = s.flushIfNeed(batch)
	s.NotifyWatcher()
	return err
}

func (s *Service) Close() error {
	s.mux.Lock()
	batch := s.batch
	s.batch = nil
	s.mux.Unlock()
	if batch == nil {
		return nil
	}
	err := s.FlushInBackground(batch)
	if err != nil {
		log.Println(err)
	}
	return err
}

func (s *Service) encoderProvider(record interface{}) (*encoder.Provider, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.encProvider != nil {
		return s.encProvider, nil
	}
	encProvider, err := encoder.New(record)
	if err != nil {
		return nil, err
	}
	s.encProvider = encProvider
	return encProvider, err
}

func (s *Service) reduce(acc *Accumulator, record interface{}) {
	key := s.keyFn(record)
	if key == "" {
		return
	}
	accumulator, ok := acc.Get(key)
	if !ok {
		accumulator = s.newRecord()
		acc.Put(key, accumulator)
	}
	s.reducerFn(accumulator, record)
}

func (s *Service) FlushInBackground(batch *Batch) error {
	batch.Mutex.Lock()
	defer func() {
		batch.Mutex.Unlock()
	}()
	if atomic.LoadUint32(&batch.flushed) == 1 {
		return nil
	}

	stats := stat.New()
	if s.flushCounter != nil {
		onDone := s.flushCounter.Begin(time.Now())
		s.flushCounter.IncrementValue(stat.Pending)
		defer func() {
			onDone(time.Now(), stats)
			s.flushCounter.DecrementValue(stat.Pending)
		}()
	}
	batch.logger.Close()

	var allErrors []error
	var loadErr error

	// prevent load when batch is empty, csv writer will return error in that case and file will never be deleted
	if batch.Accumulator.Len() != 0 {
		data := s.mapperFn(batch.Accumulator)
		loadErr = s.load(context.Background(), data, batch.ID)
	}

	if loadErr != nil {
		stats.Append(loadErr)
		allErrors = append(allErrors, loadErr)
	}

	if err2 := s.fs.Delete(context.Background(), batch.PendingURL); err2 != nil {
		stats.Append(err2)
		allErrors = append(allErrors, err2)
	}

	if batch.pendingURLSymLink != "" {
		err2b := s.fs.Delete(context.Background(), batch.pendingURLSymLink)
		if err2b != nil {
			stats.Append(err2b)
			allErrors = append(allErrors, err2b)
		}
	}

	// In cause of load err delete just pending file but not stream file
	if loadErr != nil {
		return s.joinErrors(allErrors)
	}

	if err3 := s.fs.Delete(context.Background(), batch.Stream.URL); err3 != nil {
		stats.Append(err3)
		allErrors = append(allErrors, err3)
	}

	if batch.streamURLSymLink != "" {
		err3b := s.fs.Delete(context.Background(), batch.streamURLSymLink)
		if err3b != nil {
			stats.Append(err3b)
			allErrors = append(allErrors, err3b)
		}
	}

	err := s.joinErrors(allErrors)
	if err == nil {
		atomic.StoreUint32(&batch.flushed, 1)
	}
	return err
}

func (s *Service) joinErrors(allErrors []error) error {
	var err error
	for i, e := range allErrors {
		if i == 0 {
			err = allErrors[i]
			continue
		}
		err = fmt.Errorf("%w; %w", err, e)
	}
	return err
}

func (s *Service) replayBatch(ctx context.Context, URL string, symLinkStreamURLTrgBase string) error {

	stats := stat.New()
	if s.retryCounter != nil {
		onDone := s.retryCounter.Begin(time.Now())
		s.retryCounter.IncrementValue(stat.Pending)
		defer func() {
			onDone(time.Now(), stats)
			s.retryCounter.DecrementValue(stat.Pending)
		}()
	}

	reader, err := s.fs.OpenURL(ctx, URL)
	if err != nil {
		stats.Append(err)
		return err
	}
	defer reader.Close()
	scanner := bufio.NewScanner(reader)
	processed := 0
	failed := 0
	acc := NewAccumulator()
	for scanner.Scan() {
		processed++
		data := scanner.Bytes()
		record := s.newRecord()
		if _, ok := record.(gojay.UnmarshalerJSONObject); ok {
			err = gojay.Unmarshal(data, record)
		} else {
			err = json.Unmarshal(data, record)
		}

		if err != nil {
			stats.Append(err)
			failed++
			continue
		}
		s.reduce(acc, record)
	}
	batchID := shared.BatchID(URL)
	data := s.mapperFn(acc)

	var loadErr error
	if acc.Len() != 0 {
		loadErr = s.load(ctx, data, batchID)
	}
	log.Printf("replaybatch - processed: %v, failed: %v\n", processed, failed)
	if loadErr != nil {
		stats.Append(loadErr)
		return nil // avoid deleting files
	}

	// delete regular file
	if symLinkStreamURLTrgBase != "" {
		_, fileName := url.Split(URL, file.Scheme)
		aFile := url.Join(symLinkStreamURLTrgBase, fileName)
		err = s.fs.Delete(ctx, aFile)
		if err != nil {
			stats.Append(err)
		}
	}

	// delete regular file or symlink
	if err := s.fs.Delete(ctx, URL); err != nil {
		stats.Append(err)
	}

	return nil
}

func (s *Service) load(ctx context.Context, data interface{}, batchID string) error {
	s.loadMux.Lock()
	err := s.loader.Load(ctx, data, batchID)
	go func() {
		time.Sleep(2 * time.Microsecond)
		s.loadMux.Unlock()
	}()
	return err
}

func New(config *config.Config,
	newRecord func() interface{},
	key func(record interface{}) interface{},
	reducer func(key, source interface{}),
	mapper func(accumulator *Accumulator) interface{},
	loader loader2.Loader, metrics *gmetric.Service, options ...Option) (*Service, error) {
	srv := &Service{
		config:        config,
		fs:            afs.New(),
		keyFn:         key,
		newRecord:     newRecord,
		reducerFn:     reducer,
		mapperFn:      mapper,
		notifyPending: make(chan bool, 1),
		loader:        loader,
		Provider:      msg.NewProvider(config.MaxMessageSize, config.Concurrency, tjson.New),
		metrics:       metrics,
		options:       options,
	}

	if metrics != nil {
		srv.flushCounter = metrics.MultiOperationCounter(reflect.TypeOf(srv).PkgPath(), config.ID+"Flush", "flush metric", time.Microsecond, time.Minute, 2, provider.NewBasic())
		srv.retryCounter = metrics.MultiOperationCounter(reflect.TypeOf(srv).PkgPath(), config.ID+"Retry", "retry metric", time.Microsecond, time.Minute, 2, provider.NewBasic())
	}

	err := srv.RetryFailed(context.Background(), true)
	if err == nil {
		go srv.watchInBackground()
	}
	return srv, err
}

// copy service configuration
func prepareConfig(cfg *config.Config, position int) *config.Config {
	result := *cfg
	result.ID = result.ID + strconv.Itoa(position)
	if result.Stream == nil {
		return &result
	}

	stream := *cfg.Stream
	result.Stream = &stream
	index := strings.LastIndex(result.Stream.URL, "/")
	if index > -1 {
		result.Stream.URL = result.Stream.URL[:index] + "/" + strconv.Itoa(position) + "/" + result.Stream.URL[index+1:]
	}

	return &result
}

func (s *Service) addStreamSubfolder() *config.Config {
	result := *s.config
	return &result
}

func NewCollectors(config *config.Config,
	newRecord func() interface{},
	key func(record interface{}) interface{},
	reducer func(key, source interface{}),
	mapper func(accumulator *Accumulator) interface{},
	loader loader2.Loader, metrics *gmetric.Service, count int) ([]*Service, []Collector, error) {

	if count == 0 {
		count = 1
	}

	fs := afs.New()
	URL := url.Normalize(config.Stream.URL, file.Scheme)
	parentURL, _ := url.Split(URL, file.Scheme)
	exists, err := fs.Exists(context.Background(), parentURL, file.DefaultDirOsMode, true)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, fmt.Errorf("newcollectors: folder %v does not exists", parentURL)
	}

	result := make([]*Service, count)

	err = prepareRetryOnStartUpWithLink(fs, count, config)

	if err != nil {
		return nil, nil, err
	}

	switch count {
	case 1:
		result[0], err = New(config, newRecord, key, reducer, mapper, loader, metrics)
		if err != nil {
			return nil, nil, err
		}
	default:
		for i := 0; i < count; i++ {
			aConfig := prepareConfig(config, i)
			err = ensureParentDir(aConfig, fs)
			result[i], err = New(aConfig, newRecord, key, reducer, mapper, loader, metrics, WithStreamURLSymLinkTrg(URL))
			if err != nil {
				return nil, nil, err
			}
		}
	}

	iCollectors := make([]Collector, len(result))
	for i, c := range result {
		iCollectors[i] = c
	}

	return result, iCollectors, nil
}

func ensureParentDir(aConfig *config.Config, fs afs.Service) error {
	URL := url.Normalize(aConfig.Stream.URL, file.Scheme)
	parentURL, _ := url.Split(URL, file.Scheme)
	return ensureDir(fs, parentURL)
}
