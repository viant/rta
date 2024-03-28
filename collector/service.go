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
	flushCounter *gmetric.Operation
	retryCounter *gmetric.Operation
	loadMux      sync.Mutex
	pendingRetry int32
	metrics      *gmetric.Service
}

func (s *Service) watchInBackground() {
	sleepTime := 700 * time.Millisecond

	for {
		time.Sleep(sleepTime)
		b := s.batch
		if b == nil || b.IsActive(s.config.Batch) {
			continue
		}
		time.Sleep(100 * time.Millisecond) // extra time to minimize risk of using batch in Collect fn during flushing
		_ = s.flushIfNeed(s.batch)
	}
}

func (s *Service) RetryFailed(ctx context.Context, onStartUp bool) error {
	if !atomic.CompareAndSwapInt32(&s.pendingRetry, 0, 1) {
		return nil
	}

	toRetry, err := s.loadFailedBatches(ctx, onStartUp)
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
			if err := s.replayBatch(ctx, URL); err != nil {
				errors.Add(err)
			}
			<-rateLimiter
		}(URL)
	}
	wg.Wait()
	atomic.StoreInt32(&s.pendingRetry, 0)
	return errors.First()
}

func (s *Service) loadFailedBatches(ctx context.Context, onStartUp bool) ([]string, error) {
	URL := url.Normalize(s.config.Stream.URL, file.Scheme)
	parentURL, name := url.Split(URL, file.Scheme)
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
		if !strings.HasPrefix(candidate.Name(), name) {
			continue
		}

		if strings.HasSuffix(candidate.URL(), shared.PendingSuffix) {
			if onStartUp {
				_ = s.fs.Delete(ctx, candidate.URL())
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
		b := s.flushIfNeed(batch)
		if b != nil {
			return b, nil
		}
	}
	batch, err := NewBatch(s.config.Stream, s.fs)
	if err != nil {
		return nil, err
	}
	s.batch = batch
	return batch, nil
}

func (s *Service) flushIfNeed(batch *Batch) *Batch {
	if batch.IsActive(s.config.Batch) {
		return batch
	}
	if atomic.LoadUint32(&batch.flushStarted) == 1 {
		return nil
	}

	go func() {
		if err := s.FlushInBackground(batch); err != nil {
			log.Println(err)
		}
	}()
	return nil
}

func (s *Service) Collect(record interface{}) error {
	batch, err := s.getBatch()
	if err != nil {
		return err
	}

	batch.Mutex.Lock()
	defer batch.Mutex.Unlock()

	s.reduce(batch.Accumulator, record)
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
	if !atomic.CompareAndSwapUint32(&batch.flushStarted, 0, 1) {
		return nil
	}
	batch.Mutex.Lock()
	defer batch.Mutex.Unlock()
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

	// prevent load when batch is empty, csv writer will return error in that case
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

	// In cause of load err do return, after deleting pending file (prevent deleting stream file)
	if loadErr != nil {
		return s.joinErrors(allErrors)
	}

	if err3 := s.fs.Delete(context.Background(), batch.Stream.URL); err3 != nil {
		stats.Append(err3)
		allErrors = append(allErrors, err3)
	}

	err := s.joinErrors(allErrors)
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

func (s *Service) replayBatch(ctx context.Context, URL string) error {

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
	err = s.load(ctx, data, batchID)
	if err != nil {
		stats.Append(err)
	}
	log.Printf("replaybatch - processed: %v, failed: %v\n", processed, failed)
	if err == nil {
		_ = s.fs.Delete(ctx, URL)
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
	loader loader2.Loader, metrics *gmetric.Service) (*Service, error) {
	srv := &Service{
		config:    config,
		fs:        afs.New(),
		keyFn:     key,
		newRecord: newRecord,
		reducerFn: reducer,
		mapperFn:  mapper,
		loader:    loader,
		Provider:  msg.NewProvider(config.MaxMessageSize, config.Concurrency, tjson.New),
		metrics:   metrics,
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

	fs := afs.New()
	URL := url.Normalize(config.Stream.URL, file.Scheme)
	parentURL, _ := url.Split(URL, file.Scheme)
	_, err := fs.List(context.Background(), parentURL)
	if err != nil {
		return nil, nil, err
	}

	result := make([]*Service, count)

	for i := 0; i < count; i++ {
		aConfig := prepareConfig(config, i)
		err = ensureParentDir(aConfig, fs)
		result[i], err = New(aConfig, newRecord, key, reducer, mapper, loader, metrics)
		if err != nil {
			return nil, nil, err
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
	_, err := fs.List(context.Background(), parentURL)
	if err != nil {
		if err = fs.Create(context.Background(), parentURL, file.DefaultDirOsMode, true); err != nil {
			return err
		}
	}
	return nil
}
