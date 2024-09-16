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
	"math/rand"
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
	config         *config.Config
	fs             afs.Service
	newRecord      func() interface{}
	keyFn          func(record interface{}) interface{}
	reducerFn      func(accumulator, source interface{})
	mapperFn       func(acc *Accumulator) interface{}
	loader         loader2.Loader
	batch          *Batch
	flushScheduled []*Batch
	mux            sync.RWMutex
	encProvider    *encoder.Provider
	*msg.Provider
	flushCounter        *gmetric.Operation
	retryCounter        *gmetric.Operation
	loadMux             sync.Mutex
	pendingRetry        int32
	notificationCounter int32
	metrics             *gmetric.Service
	options             []Option
	instanceId          string
	closed              int32
	randGenerator       *rand.Rand
	loadCount           int
	fastMapPool         *FMapPool
}

func (s *Service) NotifyWatcher() {
	atomic.StoreInt32(&s.notificationCounter, 1)
}

func (s *Service) watchScheduledBatches() {
	sleepTime := 100 * time.Millisecond
	for s.IsUp() {
		flushed, err := s.flushScheduledBatches()
		if err != nil {
			log.Println(err)
		}
		if flushed {
			continue
		}
		time.Sleep(sleepTime)
	}
}

func (s *Service) RetryFailed(ctx context.Context, onStartUp bool) error {
	if s.config.StreamDisabled {
		return nil
	}

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
	s.mux.RLock()
	prevBatch := s.batch
	s.mux.RUnlock()
	if prevBatch != nil && prevBatch.IsActive(s.config.Batch) {
		return prevBatch, nil
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	prevBatch = s.batch
	if prevBatch != nil && prevBatch.IsActive(s.config.Batch) {
		return prevBatch, nil
	}

	batch, err := NewBatch(s.config.Stream, s.config.StreamDisabled, s.fs, s.options...)
	if err != nil {
		return nil, err
	}
	if prevBatch != nil {
		s.flushScheduled = append(s.flushScheduled, prevBatch)
	}
	s.batch = batch

	return batch, nil
}

func (s *Service) Collect(record interface{}) error {
	return s.CollectAll(record)
}

func (s *Service) CollectAll(records ...interface{}) error {
	batch, err := s.getBatch()
	if err != nil {
		return err
	}
	atomic.AddInt32(&batch.collecting, 1)
	defer func() {
		atomic.AddInt32(&batch.collecting, -1)
	}()
	if atomic.LoadUint32(&batch.flushStarted) == 1 {
		batch, err = s.getBatch()
		if err != nil {
			return err
		}
	}
	data := batch.Accumulator
	for i := range records {
		s.reduce(data, records[i])
		if s.config.IsStreamEnabled() {
			err = s.backupLogEntry(records[i], batch)
		}
	}
	return err
}

func (s *Service) backupLogEntry(record interface{}, batch *Batch) error {
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
	if err := batch.logger.Log(message); err != nil {
		return err
	}
	message.Free()
	return nil
}

func (s *Service) Close() error {
	s.mux.Lock()
	batch := s.batch
	s.batch = nil
	s.mux.Unlock()
	if batch == nil {
		return nil
	}
	atomic.AddInt32(&s.closed, 1)
	err := s.Flush(batch)
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

func (s *Service) Flush(batch *Batch) error {
	if !atomic.CompareAndSwapUint32(&batch.flushStarted, 0, 1) {
		return nil
	}
	batch.Mutex.Lock()
	defer batch.Mutex.Unlock()
	for i := 0; i < 100; i++ {
		if atomic.LoadInt32(&batch.collecting) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
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
	if batch.logger != nil {
		batch.logger.Close()
	}
	var allErrors []error
	var loadErr error

	// prevent load when batch is empty, csv writer will return error in that case and file will never be deleted
	if batch.Accumulator.Len() != 0 {
		data := s.mapperFn(batch.Accumulator)
		loadErr = s.load(context.Background(), data, batch.ID)
		acc := batch.Accumulator
		if acc.FastMap != nil {
			s.fastMapPool.Put(acc.FastMap)
		}
	}

	if loadErr != nil {
		stats.Append(loadErr)
		allErrors = append(allErrors, loadErr)
	}

	if s.config.StreamDisabled {
		return loadErr
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
	acc := NewAccumulator(s.fastMapPool)
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
	delay := s.ensureLoadDelayIfNeeded()
	if delay != time.Duration(0) {
		time.Sleep(delay)
	}

	err := s.loader.Load(ctx, data, batchID, loader2.WithInstanceId(s.instanceId))
	s.loadCount++
	go func() {
		time.Sleep(2 * time.Microsecond)
		s.loadMux.Unlock()
	}()
	return err
}

func (s *Service) ensureLoadDelayIfNeeded() time.Duration {
	if s.randGenerator == nil || s.config.LoadDelayMaxMs == 0 {
		return time.Duration(0)
	}

	switch {
	case s.config.LoadDelayOnlyOnce && s.loadCount == 0:
	case !s.config.LoadDelayOnlyOnce && s.config.LoadDelayEveryNExec == 1:
	case !s.config.LoadDelayOnlyOnce && s.config.LoadDelayEveryNExec > 0 && s.loadCount%s.config.LoadDelayEveryNExec == 0:
	default:
		return time.Duration(0)
	}

	loadDelay := s.randGenerator.Intn(s.config.LoadDelayMaxMs)
	return time.Duration(loadDelay) * time.Millisecond
}

func New(config *config.Config,
	newRecord func() interface{},
	key func(record interface{}) interface{},
	reducer func(key, source interface{}),
	mapper func(accumulator *Accumulator) interface{},
	loader loader2.Loader, metrics *gmetric.Service, options ...Option) (*Service, error) {
	srv := &Service{
		config:     config,
		fs:         afs.New(),
		keyFn:      key,
		newRecord:  newRecord,
		reducerFn:  reducer,
		mapperFn:   mapper,
		loader:     loader,
		Provider:   msg.NewProvider(config.MaxMessageSize, config.Concurrency, tjson.New),
		metrics:    metrics,
		options:    options,
		instanceId: NewOptions(options...).GetInstanceId(),
	}

	if config.UseFastMap {
		srv.fastMapPool = NewFMapPool(max(100, config.FastMapSize), 2)
	}
	if config.LoadDelayMaxMs > 0 {
		seed := time.Now().UnixNano() + int64(config.LoadDelaySeedPart)
		srv.randGenerator = rand.New(rand.NewSource(seed))
	}

	if metrics != nil {
		srv.flushCounter = metrics.MultiOperationCounter(reflect.TypeOf(srv).PkgPath(), config.ID+"Flush", "flush metric", time.Microsecond, time.Minute, 2, provider.NewBasic())
		srv.retryCounter = metrics.MultiOperationCounter(reflect.TypeOf(srv).PkgPath(), config.ID+"Retry", "retry metric", time.Microsecond, time.Minute, 2, provider.NewBasic())
	}

	err := srv.RetryFailed(context.Background(), true)
	if err == nil {
		go srv.watchScheduledBatches()
		go srv.watchActiveBatch()
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

func (s *Service) IsUp() bool {
	return atomic.LoadInt32(&s.closed) == 0
}

func (s *Service) flushScheduledBatches() (flushed bool, err error) {
	defer func() {
		if r := recover(); err != nil {
			err = fmt.Errorf("failed to flush batch: %v", r)
		}
	}()
	s.mux.RLock()
	size := len(s.flushScheduled)
	s.mux.RUnlock()

	if size == 0 {
		return false, nil
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	batch := s.flushScheduled[0]
	if atomic.LoadInt32(&batch.collecting) == 1 {
		return false, nil
	}
	s.flushScheduled = s.flushScheduled[1:]
	err = s.Flush(batch)
	return true, err
}

func (s *Service) watchActiveBatch() {
	sleepTime := 100 * time.Millisecond
	for s.IsUp() {
		s.mux.RLock()
		batch := s.batch
		s.mux.RUnlock()
		if batch == nil {
			time.Sleep(sleepTime)
			continue
		}

		if !batch.IsActive(s.config.Batch) && atomic.LoadInt32(&batch.collecting) == 0 {
			s.mux.Lock()
			s.flushScheduled = append(s.flushScheduled, batch)
			s.batch = nil
			s.mux.Unlock()
		}

		time.Sleep(sleepTime)
	}

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
			result[i], err = New(aConfig, newRecord, key, reducer, mapper, loader, metrics, WithStreamURLSymLinkTrg(URL), WithInstanceId(strconv.Itoa(i)))
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
