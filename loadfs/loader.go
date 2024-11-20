package loadfs

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/rta/collector/loader"
	"github.com/viant/rta/loadfs/config"
	tconfig "github.com/viant/tapper/config"
	"github.com/viant/tapper/io"
	"github.com/viant/tapper/io/encoder"
	"github.com/viant/tapper/log"
	"github.com/viant/tapper/msg"
	tjson "github.com/viant/tapper/msg/json"
	"reflect"
	"sync"
	"time"
)

// Service represents a file system loader service
type Service struct {
	config      *config.Config
	fs          afs.Service
	format      Format
	msgProvider *msg.Provider
	mux         sync.RWMutex
	encProvider *encoder.Provider
}

// Load loads data into the file system
func (s *Service) Load(ctx context.Context, data interface{}, batchID string, options ...loader.Option) error {
	logger, err := s.logger(options, batchID)
	if err != nil {
		return err
	}

	defer closeWithErrorHandling(logger, &err)

	err = s.load(data, logger)
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) load(data interface{}, logger *log.Logger) error {
	val := reflect.ValueOf(data)
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("rta loader: unable to load - data is not a slice")
	}

	var record interface{}

	for i := 0; i < val.Len(); i++ {
		record = val.Index(i).Interface()
		err := s.loadEntry(record, logger)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) logger(options []loader.Option, batchID string) (*log.Logger, error) {
	t := time.Now().UTC()
	collectorInstanceId := loader.NewOptions(options...).GetInstanceId()
	category := loader.NewOptions(options...).Category()
	URL := s.format.expandURL(s.config.URL, &t, batchID, collectorInstanceId, s.format.hostIpInfo, category)

	aConfig := &tconfig.Stream{
		FlushMod:     s.config.FlushMod,
		URL:          URL,
		StreamUpload: s.config.StreamUpload,
	}

	result, err := log.New(aConfig, "", afs.New())
	if err != nil {
		return nil, fmt.Errorf("rta loader: unable to create logger due to: %w", err)
	}

	return result, nil
}

func (s *Service) loadEntry(record interface{}, logger *log.Logger) error {
	message := s.msgProvider.NewMessage()
	defer message.Free()

	enc, ok := record.(io.Encoder)
	if !ok {
		provider, err := s.encoderProvider(record)
		if err != nil {
			return err
		}
		enc = provider.New(record)
	}

	enc.Encode(message)

	if err := logger.Log(message); err != nil {
		return fmt.Errorf("rta loader: unable to log data due to: %w", err)
	}

	return nil
}

func (s *Service) encoderProvider(record interface{}) (*encoder.Provider, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.encProvider != nil {
		return s.encProvider, nil
	}
	encProvider, err := encoder.New(record)
	if err != nil {
		return nil, fmt.Errorf("rta loader: unable to create encProvider due to: %w", err)
	}
	s.encProvider = encProvider
	return encProvider, nil
}

// New creates a new file system loader service
func New(cfg *config.Config) (*Service, error) {
	err := cfg.Init()
	if err != nil {
		return nil, err
	}

	err = cfg.Validate()
	if err != nil {
		return nil, err
	}

	srv := &Service{
		config: cfg,
		format: Format{},
	}

	err = srv.init()
	if err != nil {
		return nil, err
	}

	return srv, err
}

func (s *Service) init() error {
	s.fs = afs.New()
	s.msgProvider = msg.NewProvider(s.config.MaxMessageSize, s.config.Concurrency, tjson.New)
	return s.format.Init(s.config.URL)
}

func closeWithErrorHandling(l *log.Logger, err *error) {
	if l == nil {
		return
	}

	if cerr := l.Close(); cerr != nil {
		if *err != nil {
			*err = fmt.Errorf("%w; also failed to close: %v", *err, cerr)
		} else {
			*err = cerr
		}
	}
}
