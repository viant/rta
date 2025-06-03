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
	keyPtrFn       func(record interface{}, dest interface{})
	keyFn          func(record interface{}) interface{}
	reducerFn      func(accumulator, source interface{})
	mapperFn       func(acc *Accumulator) interface{}
	loader         loader2.Loader
	fsLoader       loader2.Loader
	activeBatch    *Batch
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
	mapPool             *MapPool
	category            string
	useShardedAcc       bool // if true, use sharded accumulator
	shardAccPool        *ShardAccPool
}

func (s *Service) NotifyWatcher() {
	atomic.StoreInt32(&s.notificationCounter, 1)
}

func (s *Service) watchScheduledBatches() {
	sleepTime := 100 * time.Millisecond
	for s.IsUp() {
		flushed, err := s.flushScheduledBatches(context.Background())
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
	prevBatch := s.activeBatch
	s.mux.RUnlock()
	if prevBatch != nil && prevBatch.IsActive(s.config.Batch) {
		return prevBatch, nil
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	prevBatch = s.activeBatch
	if prevBatch != nil && prevBatch.IsActive(s.config.Batch) {
		return prevBatch, nil
	}
	if s.fastMapPool != nil {
		s.options = append(s.options, WithFastMapPool(s.fastMapPool))
	}
	if s.mapPool != nil {
		s.options = append(s.options, WithMapPool(s.mapPool))
	}
	if s.shardAccPool != nil {
		s.options = append(s.options, WithShardAccPool(s.shardAccPool))
	}

	batch, err := NewBatch(s.config.Stream, s.config.StreamDisabled, s.fs, s.options...)
	if err != nil {
		return nil, err
	}
	if prevBatch != nil {
		s.scheduleBatch(false, prevBatch)
	}
	s.activeBatch = batch
	return batch, nil
}

func (s *Service) GetBatch() (*Batch, error) {
	return s.getBatch()
}

func (s *Service) ID() string {
	return s.config.ID
}

func (s *Service) Collect(record interface{}) error {
	return s.CollectAll(record)
}

func (s *Service) CollectAll(records ...interface{}) error {
	batch, err := s.getBatch()
	if err != nil {
		fmt.Printf("failed to get batch: %v\n", err)
		return err
	}
	atomic.AddInt32(&batch.collecting, 1)
	defer func() {
		atomic.AddInt32(&batch.collecting, -1)
	}()
	err = s.collectAll(records, batch)
	if err != nil {
		fmt.Printf("failed to collect batch: %v\n", err)
	}

	return err
}

func (s *Service) collectAll(records []interface{}, batch *Batch) error {
	data := batch.Accumulator
	if len(records) == 0 {
		return nil
	}

	for i := range records { //collect all to memory
		s.reduce(data, records[i])
	}
	if s.config.StreamDisabled {
		return nil
	}
	for i := range records {
		if err := s.backupLogEntry(records[i], batch); err != nil { //io error
			bErr := &BackupError{Total: len(records), Failed: len(records) - i, err: err}
			return bErr
		}

	}
	return nil
}

type BackupError struct {
	Total  int
	Failed int
	err    error
}

func (b *BackupError) Error() string {
	return fmt.Sprintf("failed to backup log: failed: %v of %v, due to %v", b.Failed, b.Total, b.err)
}

func (s *Service) backupLogEntry(record interface{}, batch *Batch) error {
	message := s.Provider.NewMessage()
	enc, ok := record.(io.Encoder)
	if !ok {
		aProvider, err := s.encoderProvider(record)
		if err != nil {
			return err
		}
		enc = aProvider.New(record)
	}
	enc.Encode(message)

	// just for testing purpose
	//if atomic.AddInt32(&testCounter, 1) > 10000 && atomic.LoadInt32(&testCounter) < 40000 && atomic.LoadInt32(&testCounter)%2 == 1 {
	//	return fmt.Errorf("device full i/o")
	//}

	if err := batch.logger.Log(message); err != nil {
		return err
	}
	message.Free()
	return nil
}

func (s *Service) Close() error {
	s.mux.Lock()
	batch := s.activeBatch
	s.activeBatch = nil
	s.mux.Unlock()
	if batch == nil {
		return nil
	}
	atomic.AddInt32(&s.closed, 1)
	_, err := s.Flush(batch)
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
	if key == nil {
		return
	}
	accumulator, ok := acc.GetOrCreate(key, s.newRecord)
	if accumulator == nil {
		log.Printf("recover race condition %v %v\n", accumulator, ok)
		accumulator = acc.Put(key, s.newRecord())
	}
	s.reducerFn(accumulator, record)
}

func (s *Service) Flush(batch *Batch) (string, error) {
	if !atomic.CompareAndSwapUint32(&batch.flushStarted, 0, 1) {
		return "", nil
	}
	ctx := context.Background()
	batch.ensureNoPending()
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

	primaryElapsed := time.Duration(0)
	secondaryElapsed := time.Duration(0)
	primarySubLog := ""
	flushLog := ""

	// prevent load when batch is empty, csv writer will return error in that case and file will never be deleted
	batchLen := batch.Accumulator.Len()
	if batchLen != 0 {
		data := s.mapperFn(batch.Accumulator)
		startPrimary := time.Now()
		var err error
		if primarySubLog, err = s.load(context.Background(), data, batch.ID); err != nil {
			atomic.StoreUint32(&batch.flushStarted, 0)
			err2 := fmt.Errorf("load error (collector [instance: %s, category: %s], rows cnt: %d, batch id: %s) due to: %w", s.instanceId, s.category, batchLen, batch.ID, err)
			stats.Append(err2)
			return "", err2
		}
		primaryElapsed = time.Now().Sub(startPrimary)

		startSecondary := time.Now()
		if s.fsLoader != nil {
			err := s.fsLoader.Load(ctx, data, batch.ID, loader2.WithInstanceId(s.instanceId), loader2.WithCategory(s.category))
			err, retries := s.retryFsLoadIfNeeded(batch.ID, err, ctx, data)
			if err != nil {
				err2 := fmt.Errorf("flush warning: failed to fs load (collector [instance: %s, category: %s], rows cnt: %d, batch id: %s%s) due to: %w",
					s.instanceId, s.category, batchLen, batch.ID, retries, err)
				log.Println(err2.Error())
				stats.Append(err2)
			}
		}
		secondaryElapsed = time.Now().Sub(startSecondary)
	}

	if s.config.Debug {
		flushLog = fmt.Sprintf("pLoad:%v, sLoad:%v, %s", primaryElapsed, secondaryElapsed, primarySubLog)
	}

	return flushLog, s.closeBatch(ctx, batch)

}

func (s *Service) retryFsLoadIfNeeded(batchID string, err error, ctx context.Context, data interface{}) (error, string) {
	if err == nil || s.config.FsLoaderMaxRetry == 0 {
		return nil, ""
	}

	delay := time.Duration(s.config.FsLoaderRetryDelayMs) * time.Millisecond
	for i := 0; i < s.config.FsLoaderMaxRetry; i++ {
		time.Sleep(delay)
		err = s.fsLoader.Load(ctx, data, batchID, loader2.WithInstanceId(s.instanceId), loader2.WithCategory(s.category))
		if err == nil {
			return nil, ""
		}
	}

	return err, fmt.Sprintf(", exceeded number of retries: %d", s.config.FsLoaderMaxRetry)
}

func (s *Service) closeBatch(ctx context.Context, batch *Batch) error {
	var err error
	if s.config.IsStreamEnabled() {
		if batch.logger != nil {
			_ = batch.logger.Close()
		}
		var allErrors []error
		if err := batch.removePendingFile(ctx, s.fs); err != nil {
			allErrors = append(allErrors, err)
		}
		if err := batch.removeDataFile(ctx, s.fs); err != nil {
			allErrors = append(allErrors, err)
		}
		err = s.joinErrors(allErrors)
	}
	acc := batch.Accumulator
	if acc.FastMap != nil {
		s.fastMapPool.Put(acc.FastMap)
	}

	if acc.Map != nil {
		if s.mapPool != nil {
			s.mapPool.Put(acc.Map)
		}
		acc.Map = make(map[interface{}]interface{})
	}

	if s.useShardedAcc && acc.ShardedAccumulator != nil {
		s.shardAccPool.Put(acc.ShardedAccumulator)
		acc = nil
		batch = nil
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
	acc := NewAccumulator(s.fastMapPool, s.mapPool, s.shardAccPool)
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
		_, loadErr = s.load(ctx, data, batchID)
	}
	log.Printf("replaybatch - processed: %v, failed: %v url: %v\n", processed, failed, URL)
	if loadErr != nil {
		stats.Append(loadErr)
		return nil // avoid deleting files
	}

	if s.fsLoader != nil {
		err := s.fsLoader.Load(ctx, data, batchID, loader2.WithInstanceId(s.instanceId), loader2.WithCategory(s.category))
		err, retries := s.retryFsLoadIfNeeded(batchID, err, ctx, data)
		if err != nil {
			err2 := fmt.Errorf("replaybatch warning: failed to fs load (collector [instance: %s, category: %s], rows cnt: %d, batch id: %s%s) due to: %w", s.instanceId, s.category, acc.Len(), batchID, retries, err)
			log.Println(err2.Error())
			stats.Append(err2)
		}
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
func (s *Service) Config() *config.Config {
	return s.config
}

func (s *Service) load(ctx context.Context, data interface{}, batchID string) (string, error) {
	s.loadMux.Lock()
	delay := s.ensureLoadDelayIfNeeded()
	if delay != time.Duration(0) {
		time.Sleep(delay)
	}

	loadLog := ""
	err := s.loader.Load(ctx, data, batchID, loader2.WithInstanceId(s.instanceId), loader2.WithCategory(s.category))

	s.loadCount++
	go func() {
		time.Sleep(2 * time.Microsecond)
		s.loadMux.Unlock()
	}()

	if s.config.Debug {
		loadLog = fmt.Sprintf("pDelay:%v", delay)
	}

	return loadLog, err
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

func New(cfg *config.Config,
	newRecord func() interface{},
	key func(record interface{}) interface{},
	reducer func(key, source interface{}),
	mapper func(accumulator *Accumulator) interface{},
	loader loader2.Loader, metrics *gmetric.Service, options ...Option) (*Service, error) {
	opts := NewOptions(options...)

	fsLoader := opts.FsLoader()

	srv := &Service{
		config:     cfg,
		fs:         afs.New(),
		keyFn:      key,
		newRecord:  newRecord,
		reducerFn:  reducer,
		mapperFn:   mapper,
		loader:     loader,
		fsLoader:   fsLoader,
		Provider:   msg.NewProvider(cfg.MaxMessageSize, cfg.Concurrency, tjson.New),
		metrics:    metrics,
		options:    options,
		instanceId: opts.GetInstanceId(),
		keyPtrFn:   opts.keyPtrFn,
		category:   opts.Category(),
	}

	if cfg.UseFastMap {
		srv.fastMapPool = NewFMapPool(max(100, cfg.FastMapSize), 2)
	}

	if cfg.UseShardAccumulator {
		srv.useShardedAcc = cfg.UseShardAccumulator
		if cfg.ShardCnt <= 0 {
			cfg.ShardCnt = 10
		}
		srv.shardAccPool = NewShardAccPool(100, cfg.ShardCnt)
	}

	if cfg.MapPoolCfg != nil {
		srv.mapPool = NewMapPool(cfg.MapPoolCfg.MapInitSize)
	}

	if cfg.LoadDelayMaxMs > 0 {
		seed := time.Now().UnixNano() + int64(cfg.LoadDelaySeedPart)
		srv.randGenerator = rand.New(rand.NewSource(seed))
	}

	if metrics != nil {
		srv.flushCounter = metrics.MultiOperationCounter(reflect.TypeOf(srv).PkgPath(), cfg.ID+"Flush", "flush metric", time.Microsecond, time.Minute, 2, provider.NewBasic())
		srv.retryCounter = metrics.MultiOperationCounter(reflect.TypeOf(srv).PkgPath(), cfg.ID+"Retry", "retry metric", time.Microsecond, time.Minute, 2, provider.NewBasic())
	}

	err := srv.RetryFailed(context.Background(), true)
	if err != nil {
		return nil, err
	}

	switch cfg.Mode {
	case config.ManualMode:
		// do nothing
	default:
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

func (s *Service) FlushOnDemand(ctx context.Context, batch *Batch, cnt int, startFlushScheduledBatches *time.Time) error {
	elapsedMergeBatch := time.Now().Sub(*startFlushScheduledBatches)
	batchCnt := batch.Accumulator.Len()
	flushSubLog, err := s.Flush(batch)
	elapsedTotal := time.Now().Sub(*startFlushScheduledBatches)
	if err == nil && s.config.Debug {
		fmt.Printf("successfully flushed %s rows:%d, batchCnt:%d, total:%v, merge:%v, %v, %v\n",
			s.instanceId,
			batchCnt, cnt,
			elapsedTotal, elapsedMergeBatch,
			flushSubLog,
			batch.ID)
	}

	return err
}

func (s *Service) flushScheduledBatches(ctx context.Context) (flushed bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			//	debug.PrintStack()
			err = fmt.Errorf("goroutine panic: %v", r)
		}
		if err != nil {
			err = fmt.Errorf("failed to flush batch due to: %w", err)
		}

	}()
	startFlushScheduledBatches := time.Now()
	s.mux.RLock()
	size := len(s.flushScheduled)
	s.mux.RUnlock()
	if size == 0 {
		return false, nil
	}
	batchesToFlushNow := s.getBatchesToFlush(size)
	if len(batchesToFlushNow) == 0 {
		return false, nil
	}
	masterBatch := batchesToFlushNow[0]

	startMergeBatch := time.Now()
	var inconsistentBackup []*Batch
	for i := 1; i < len(batchesToFlushNow); i++ {
		candidate := batchesToFlushNow[i]
		if err := s.mergeBatches(ctx, masterBatch, candidate); err != nil {
			if i+1 >= len(batchesToFlushNow) {
				s.scheduleBatch(true, batchesToFlushNow[i+1:]...)
			}
			inconsistentBackup = append(inconsistentBackup, candidate)
			fmt.Println("failed to merge batch, scheduling for flush", err)
			break
		}
	}
	elapsedMergeBatch := time.Now().Sub(startMergeBatch)

	masterCnt := masterBatch.Accumulator.Len()
	flushSubLog, err := s.Flush(masterBatch)

	elapsedTotal := time.Now().Sub(startFlushScheduledBatches)
	if err != nil { //if flush failed, lets put back the batch to the flushScheduled
		s.scheduleBatch(true, masterBatch)
	}
	if err == nil {
		if s.config.Debug {
			fmt.Printf("successfully flushed [%s, %s] rows:%d, batchCnt:%d, total:%v, merge:%v, %v, %v\n",
				s.instanceId, s.category,
				masterCnt, len(batchesToFlushNow),
				elapsedTotal, elapsedMergeBatch,
				flushSubLog,
				masterBatch.ID)
		}
		for i, item := range inconsistentBackup {
			if err := s.closeBatch(ctx, item); err != nil {
				fmt.Printf("failed to close insonsisten activeBatch %v/%v, %v", i, len(inconsistentBackup), err)
			}
		}
	}
	return true, err
}

func (s *Service) getBatchesToFlush(size int) []*Batch {
	batchesToFlushNow := make([]*Batch, 0, size)
	pendingTransactionBatches := make([]*Batch, 0, size)
	s.mux.Lock()
	defer s.mux.Unlock()
	ts := time.Now()
	for i, candidate := range s.flushScheduled {
		if candidate.IsReadyForFlush(&ts) {
			batchesToFlushNow = append(batchesToFlushNow, s.flushScheduled[i])
		} else {
			pendingTransactionBatches = append(pendingTransactionBatches, s.flushScheduled[i])
		}
	}
	s.flushScheduled = pendingTransactionBatches
	return batchesToFlushNow
}

func (s *Service) scheduleBatch(withLock bool, batches ...*Batch) {
	t := time.Now()
	t.Add(time.Second)
	for _, batch := range batches {
		batch.ScheduleAt(&t)
	}
	if withLock {
		s.mux.Lock()
	}
	s.flushScheduled = append(s.flushScheduled, batches...)
	if withLock {
		s.mux.Unlock()
	}
}

func (b *Batch) ScheduleAt(ts *time.Time) {
	if b.flushAfter != nil {
		return
	}
	b.flushAfter = ts
}

func (s *Service) watchActiveBatch() {
	sleepTime := 100 * time.Millisecond
	for s.IsUp() {
		s.mux.Lock()
		batch := s.activeBatch
		if batch == nil {
			s.mux.Unlock()
			time.Sleep(sleepTime)
			continue
		}

		if !batch.IsActive(s.config.Batch) && !batch.HasPendingTransaction() {
			s.scheduleBatch(false, batch)
			s.activeBatch = nil
		}
		s.mux.Unlock()
		time.Sleep(sleepTime)
	}

}

func (s *Service) mergeBatches(ctx context.Context, dest *Batch, from *Batch) error {
	var items []interface{}
	if from.Accumulator.ShardedAccumulator != nil {
		items = make([]interface{}, 0, from.Accumulator.ShardedAccumulator.Len())
		for _, sh := range from.Accumulator.ShardedAccumulator.Shards {
			for _, value := range sh.M {
				items = append(items, value)
			}
		}
	} else if from.Accumulator.UseFastMap {
		items = make([]interface{}, 0, from.Accumulator.FastMap.Count())
		from.Accumulator.FastMap.Iter(func(k any, value any) (stop bool) {
			items = append(items, value)
			return false
		})
	} else if len(from.Accumulator.Map) > 0 {
		items = make([]interface{}, 0, len(from.Accumulator.Map))
		for _, value := range from.Accumulator.Map {
			items = append(items, value)
		}
	}

	err := s.collectAll(items, dest)
	if err != nil {
		fmt.Printf("failed to merge to master batch with error: %v, %v\n", len(items), err)
	}

	if err != nil {
		return err
	}
	if err := s.closeBatch(ctx, from); err != nil {
		fmt.Printf("failed to close item merged batched: %v\n", err)
	}
	return nil
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
