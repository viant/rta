package loadfs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/viant/afs"
	"github.com/viant/rta/collector/loader"
	"github.com/viant/rta/domain"
	"github.com/viant/rta/loadfs/config"
	"github.com/viant/rta/shared"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/option"
	tconfig "github.com/viant/tapper/config"
	"github.com/viant/tapper/io"
	"github.com/viant/tapper/io/encoder"
	"github.com/viant/tapper/log"
	"github.com/viant/tapper/msg"
	tjson "github.com/viant/tapper/msg/json"
)

// Service represents a file system loader service
type Service struct {
	config           *config.Config
	hostIP           string
	fs               afs.Service
	format           Format
	msgProvider      *msg.Provider
	mux              sync.RWMutex
	encProvider      *encoder.Provider
	wrapFn           func(record interface{}, wrapper interface{}) (interface{}, error)
	metaSessionCache *sync.Map
}

// Load loads data into the file system
func (s *Service) Load(ctx context.Context, data interface{}, batchID string, options ...loader.Option) error {
	destURL := s.getURL(options, batchID)
	if err := s.loadWithLogger(destURL, data); err != nil {
		return err
	}
	return s.insertToJournalIfNeeded(ctx, destURL, batchID)
}

func (s *Service) loadWithLogger(URL string, data interface{}) error {
	logger, err := s.logger(URL)
	if err != nil {
		return err
	}

	err = s.load(data, logger)
	errors.Join(err, logger.Close())

	return err
}

func (s *Service) insertToJournalIfNeeded(ctx context.Context, destURL string, batchID string) (err error) {
	if s.config.ConnectionJn == nil {
		return nil
	}

	dbJn, metaSessionCacheKey, err := s.config.ConnectionJn.OpenDB(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dbJn.Close()) }()

	err = s.insertToJournal(ctx, dbJn, destURL, batchID, metaSessionCacheKey)
	if err != nil {
		return fmt.Errorf("failed to insert into journal table %s due to: %w", s.config.JournalTable, err)
	}

	return nil
}

func (s *Service) load(data interface{}, logger *log.Logger) error {
	val := reflect.ValueOf(data)
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("rta loader: unable to load - data is not a slice")
	}

	var record interface{}
	var wrapper interface{}
	var err error

	for i := 0; i < val.Len(); i++ {
		record = val.Index(i).Interface()

		if s.wrapFn != nil {
			// we can reuse wrapper as long everything is sequential and ephemeral (no concurrency and no references are stored )
			wrapper, err = s.wrapFn(record, wrapper)
			if err != nil {
				return err
			}
			record = wrapper
		}

		err := s.loadEntry(record, logger)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) logger(URL string) (*log.Logger, error) {

	aConfig := &tconfig.Stream{
		FlushMod:     s.config.FlushMod,
		URL:          URL,
		StreamUpload: s.config.StreamUpload,
	}

	result, err := log.New(aConfig, "", afs.New())
	if err != nil {
		return nil, fmt.Errorf("rta fs loader: unable to create logger due to: %w", err)
	}

	return result, nil
}

func (s *Service) getURL(options []loader.Option, batchID string) string {
	t := time.Now().UTC()
	opts := loader.NewOptions(options...)
	collectorInstanceId := opts.GetInstanceId()
	category := opts.Category()
	URL := s.format.expandURL(s.config.URL, &t, batchID, collectorInstanceId, s.format.hostIpInfo, category)
	return URL
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
func New(cfg *config.Config, options ...loader.Option) (*Service, error) {
	opts := loader.NewOptions(options...)

	err := cfg.Init()
	if err != nil {
		return nil, err
	}

	err = cfg.Validate()
	if err != nil {
		return nil, err
	}

	srv := &Service{
		config:           cfg,
		format:           Format{},
		wrapFn:           opts.GetWrapFunc(),
		metaSessionCache: &sync.Map{},
	}

	err = srv.init()
	if err != nil {
		return nil, err
	}

	return srv, err
}

func (s *Service) init() error {
	var err error
	s.fs = afs.New()
	s.hostIP, err = shared.GetLocalIPv4()
	if err != nil {
		return err
	}
	if s.hostIP == "::1" {
		s.hostIP = "127.0.0.1"
	}
	s.msgProvider = msg.NewProvider(s.config.MaxMessageSize, s.config.Concurrency, tjson.New)

	err = s.ensureJnTableIfNeeded()
	if err != nil {
		return err
	}

	return s.format.Init(s.config.URL)
}

func (s *Service) ensureJnTableIfNeeded() (err error) {
	if s.config.ConnectionJn == nil {
		return nil
	}

	ctx := context.Background()
	dbJn, _, err := s.config.ConnectionJn.OpenDB(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dbJn.Close()) }()

	DDL := strings.TrimSpace(s.config.CreateJnDDL)
	if len(DDL) > 0 {
		_, err = dbJn.ExecContext(ctx, DDL)
	}

	return err
}

func (s *Service) insertToJournal(ctx context.Context, db *sql.DB, destURL string, batchID string, metaSessionCacheKey string) error {
	jnTable := s.config.JournalTable
	inserter, err := insert.New(ctx, db, jnTable, option.MetaSessionCacheKey(metaSessionCacheKey), option.WithMetaSessionCache(s.metaSessionCache))
	if err != nil {
		return err
	}
	ts := time.Now()
	journal := &domain.JournalFs{
		Ip:           s.hostIP,
		BatchID:      batchID,
		Status:       shared.Active,
		TempLocation: destURL,
		Created:      &ts,
		Updated:      &ts,
	}
	_, _, err = inserter.Exec(ctx, journal, dialect.PresetIDStrategyIgnore)
	return err
}
