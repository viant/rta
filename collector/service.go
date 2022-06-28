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
	"github.com/viant/rta/collector/config"
	"github.com/viant/rta/shared"
	"github.com/viant/tapper/io"
	"sync/atomic"
	"time"

	"github.com/viant/tapper/io/encoder"
	"github.com/viant/tapper/msg"
	tjson "github.com/viant/tapper/msg/json"
	"log"
	"strings"
	"sync"
)

type Service struct {
	config      *config.Config
	fs          afs.Service
	newRecord   func() interface{}
	keyFn       func(record interface{}) interface{}
	reducerFn   func(accumulator, source interface{})
	mapperFn    func(acc *Accumulator) interface{}
	loader      Loader
	batch       *Batch
	mux         sync.Mutex
	encProvider *encoder.Provider
	*msg.Provider
	counter uint32
}

func (s *Service) watchInBackground() {
	sleepTime := time.Second
	if s.config.Batch != nil {
		sleepTime = s.config.Batch.MaxDuration()
	}
	for {
		if atomic.LoadUint32(&s.counter) == 0 {
			time.Sleep(sleepTime)
			continue
		}
		atomic.StoreUint32(&s.counter, 0)
		_, err := s.getBatch()
		if err != nil {
			log.Println(err)
		}
		time.Sleep(sleepTime)
	}
}

func (s *Service) RetryFailed(ctx context.Context, onStartUp bool) error {
	toRetry, err := s.loadFailedBatches(ctx, onStartUp)
	if err != nil || len(toRetry) == 0 {
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
	return errors.First()
}

func (s *Service) loadFailedBatches(ctx context.Context, onStartUp bool) ([]string, error) {
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
	batch, err := NewBatch(s.config.Stream, s.fs)
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
	atomic.AddUint32(&s.counter, 1)
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
	accumulator, ok := acc.Get(key)
	if !ok {
		accumulator = s.newRecord()
		acc.Put(key, accumulator)
	}
	s.reducerFn(accumulator, record)
}

func (s *Service) FlushInBackground(batch *Batch) error {
	batch.logger.Close()
	data := s.mapperFn(batch.Accumulator)
	err := s.loader.Load(context.Background(), data, batch.ID)
	_ = s.fs.Delete(context.Background(), batch.PendingURL)
	if err != nil {
		return err
	}
	err = s.fs.Delete(context.Background(), batch.Stream.URL)
	return err
}

func (s *Service) replayBatch(ctx context.Context, URL string) error {
	reader, err := s.fs.OpenURL(ctx, URL)
	if err != nil {
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
			failed++
			continue
		}
		s.reduce(acc, record)
	}
	batchID := shared.BatchID(URL)
	data := s.mapperFn(acc)
	err = s.loader.Load(ctx, data, batchID)
	fmt.Printf("processed: %v, failed: %v\n", processed, failed)
	if err == nil {
		_ = s.fs.Delete(ctx, URL)
	}
	return nil
}

func New(config *config.Config,
	newRecord func() interface{},
	key func(record interface{}) interface{},
	reducer func(key, source interface{}),
	mapper func(accumulator *Accumulator) interface{},
	loader Loader) (*Service, error) {
	srv := &Service{
		config:    config,
		fs:        afs.New(),
		keyFn:     key,
		newRecord: newRecord,
		reducerFn: reducer,
		mapperFn:  mapper,
		loader:    loader,
		Provider:  msg.NewProvider(config.MaxMessageSize, config.Concurrency, tjson.New),
	}
	err := srv.RetryFailed(context.Background(), true)
	if err == nil {
		go srv.watchInBackground()
	}
	return srv, err
}
