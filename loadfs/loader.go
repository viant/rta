package loadfs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/rta/collector/loader"
	"github.com/viant/rta/domain"
	"github.com/viant/rta/loadfs/config"
	"github.com/viant/rta/shared"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	tconfig "github.com/viant/tapper/config"
	"github.com/viant/tapper/io"
	"github.com/viant/tapper/io/encoder"
	"github.com/viant/tapper/log"
	"github.com/viant/tapper/msg"
	tjson "github.com/viant/tapper/msg/json"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Service represents a file system loader service
type Service struct {
	config      *config.Config
	hostIP      string
	fs          afs.Service
	format      Format
	msgProvider *msg.Provider
	mux         sync.RWMutex
	encProvider *encoder.Provider
}

// Load loads data into the file system
func (s *Service) Load(ctx context.Context, data interface{}, batchID string, options ...loader.Option) (err error) {
	logger, destURL, err := s.logger(options, batchID)
	if err != nil {
		return err
	}

	defer func() { err = errors.Join(err, logger.Close()) }()

	err = s.load(data, logger)
	if err != nil {
		return err
	}

	err = s.insertToJournalIfNeeded(ctx, destURL, batchID)

	return err
}

func (s *Service) insertToJournalIfNeeded(ctx context.Context, destURL string, batchID string) (err error) {
	if s.config.ConnectionJn == nil {
		return nil
	}

	dbJn, err := s.config.ConnectionJn.OpenDB(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dbJn.Close()) }()

	err = s.insertToJournal(ctx, dbJn, destURL, batchID)
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

	for i := 0; i < val.Len(); i++ {
		record = val.Index(i).Interface()
		err := s.loadEntry(record, logger)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) logger(options []loader.Option, batchID string) (*log.Logger, string, error) {
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
		return nil, "", fmt.Errorf("rta fs loader: unable to create logger due to: %w", err)
	}

	return result, URL, nil
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
	dbJn, err := s.config.ConnectionJn.OpenDB(ctx)
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

func (s *Service) insertToJournal(ctx context.Context, db *sql.DB, destURL string, batchID string) error {
	jnTable := s.config.JournalTable
	inserter, err := insert.New(ctx, db, jnTable)
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
	}
	_, _, err = inserter.Exec(ctx, journal, dialect.PresetIDStrategyIgnore)
	return err
}
