package mergefs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/gmetric"
	"github.com/viant/gmetric/provider"
	rconfig "github.com/viant/rta/config"
	"github.com/viant/rta/domain"
	"github.com/viant/rta/load"
	lconfig "github.com/viant/rta/load/config"
	"github.com/viant/rta/mergefs/config"
	"github.com/viant/rta/mergefs/handler"
	"github.com/viant/rta/mergefs/registry"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/x"
	"github.com/viant/xreflect"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	mergeOperation   = "merge"
	journalOperation = "journal"
	operationSuffix  = "operation"
)

// MultiMerger represents a multi merger
type MultiMerger struct {
	config           *config.Config
	mergers          []*Service
	metrics          *gmetric.Service
	counter          *gmetric.Operation
	journal          *gmetric.Operation
	fs               afs.Service
	xType            *x.Type
	dbJn             *sql.DB
	destPlaceholders []string
	mux              sync.Mutex
	typeDef          string
	inUse            map[string]bool
}

func (m *MultiMerger) startEndpoint() {
	if m.config.Endpoint == nil {
		return
	}
	metricHandler := gmetric.NewHandler(metricURI, m.metrics)
	http.Handle(metricURI, metricHandler)
	http.Handle(handler.ConfigURI, handler.NewHandler(m.config))
	http.HandleFunc(handler.StatusURI, handler.StatusOK)
	err := http.ListenAndServe(":"+strconv.Itoa(m.config.Endpoint.Port), http.DefaultServeMux)
	if err != nil {
		log.Fatal(err)
	}
}

// MergeInBackground run in background MergeInBackground for all mergers
func (m *MultiMerger) MergeInBackground() {
	wg := sync.WaitGroup{}

	for {
		err := m.populateMergers()
		if err != nil {
			log.Printf("%s failed to populate mergers: %v", logPrefix, err)
		}

		m.mux.Lock()
		for _, merger := range m.mergers {
			_, ok := m.inUse[merger.config.Dest]
			if ok {
				continue
			}
			m.inUse[merger.config.Dest] = true
			wg.Add(1)
			fmt.Printf("%s successfully started a new merger in background: %s\n", logPrefix, merger.config.Dest)
			go merger.MergeInBackground(&wg)

			time.Sleep(time.Millisecond * time.Duration(m.config.MainLoopDelayMs))
		}
		m.mux.Unlock()

		time.Sleep(time.Millisecond * time.Duration(m.config.MergersRefreshMs))
	}

	wg.Wait()
}

// Merge run merge function for all mergers
func (m *MultiMerger) Merge(ctx context.Context) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	var err error
	var mux sync.Mutex
	wg := sync.WaitGroup{}
	for i := range m.mergers {
		merger := m.mergers[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err2 := merger.Merge(ctx)
			if err2 != nil {
				log.Printf("%s failed to merge table %s: %v", logPrefix, merger.config.Dest, err)
				mux.Lock()
				defer mux.Unlock()
				err = errors.Join(err, err2)
			}
		}()
		time.Sleep(time.Millisecond * time.Duration(m.config.MainLoopDelayMs))
	}
	wg.Wait()
	return err
}

// New creates a new multi merger
func New(c *config.Config) (*MultiMerger, error) {
	result := &MultiMerger{
		config:  c,
		metrics: gmetric.New(),
		mergers: make([]*Service, 0),
		fs:      afs.New(),
		inUse:   make(map[string]bool),
	}

	r := registry.Registry()
	result.xType = registry.LookUp(r, c.TypeName)
	if result.xType == nil {
		return nil, fmt.Errorf("%s failed to lookup type: %v", logPrefix, c.TypeName)
	}
	result.typeDef = ensureTypeDef(result.xType.Type)

	var err error

	err = result.initJnConnection()
	if err != nil {
		return nil, err
	}

	err = result.populateMergers()
	if err != nil {
		return nil, fmt.Errorf("%s failed to populate mergers due to: %w", logPrefix, err)
	}

	go result.startEndpoint()
	return result, nil
}

func (m *MultiMerger) initJnConnection() error {
	var err error

	cfg := m.config.JournalConnection
	m.dbJn, err = cfg.OpenDB(context.Background())

	if cfg.MaxOpenConns > 0 {
		m.dbJn.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		m.dbJn.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.MaxLifetimeMs > 0 {
		m.dbJn.SetConnMaxLifetime(time.Duration(cfg.MaxLifetimeMs) * time.Millisecond)
	}
	if cfg.MaxIdleTimeMs > 0 {
		m.dbJn.SetConnMaxIdleTime(time.Duration(cfg.MaxIdleTimeMs) * time.Millisecond)
	}

	return err
}

func (m *MultiMerger) populateMergers() error {
	result, err := ensurePlaceholdersIfNeeded(m.config)
	if err != nil {
		return err
	}

	if len(result) == 0 {
		result = append(result, "") // case when there are no placeholders, and only 1 default merger is created
	}

	m.mux.Lock()
	defer m.mux.Unlock()

	newPlaceholders := difference(result, m.destPlaceholders)
	if len(newPlaceholders) == 0 {
		return nil
	}

	for _, name := range newPlaceholders {
		merger, err := m.newMerger(name)
		if err != nil { // just print error without returning
			fmt.Printf("%s failed to create merger for placeholder %q: %v\n", logPrefix, name, err)
			continue
		}
		m.mergers = append(m.mergers, merger)
		m.destPlaceholders = append(m.destPlaceholders, name)
	}
	return nil
}

func (m *MultiMerger) newMerger(name string) (*Service, error) {
	aConfig, lConfig, err := ensureConfig(m.config, name, m.typeDef)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure configuration for %q: %w", name, err)
	}

	if err := lConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid loader configuration for %q: %w", name, err)
	}

	err = checkTableExistence(m.dbJn, aConfig.JournalTable)
	if err != nil {
		return nil, err
	}

	loader, err := ensureLoader(lConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize loader for %q: %w", name, err)
	}

	srv := &Service{
		config:  aConfig,
		metrics: m.metrics,
		fs:      m.fs,
		xType:   m.xType,
		loader:  loader,
		dbJn:    m.dbJn,
	}
	ensureCounters(srv, name)

	return srv, nil
}

func ensureCounters(srv *Service, name string) {
	suffix := ""
	if name != "" {
		suffix = "_" + strings.ToLower(name)
	}
	srv.counter = srv.metrics.MultiOperationCounter(reflect.TypeOf(srv).PkgPath(), mergeOperation+suffix, mergeOperation+" "+operationSuffix, time.Microsecond, time.Minute, 2, provider.NewBasic())
	srv.journal = srv.metrics.MultiOperationCounter(reflect.TypeOf(srv).PkgPath(), journalOperation+suffix, journalOperation+" "+mergeOperation+" "+operationSuffix, time.Microsecond, time.Minute, 2, provider.NewBasic())
}

func ensureLoaderConfig(c *config.Config) (lConfig *lconfig.Config, err error) {
	ctx := context.Background()
	product, err := detectProduct(ctx, c.Connection)
	if err != nil {
		return nil, err
	}

	onDuplicateKeySql, err := onDuplicateKey(product, c)
	if err != nil {
		return nil, err
	}

	lConfig = &lconfig.Config{
		Mode:              c.Mode,
		Dest:              c.Dest,
		CreateDDL:         c.CreateDDL,
		UseInsertAPI:      c.UseInsertAPI,
		OnDuplicateKeySql: onDuplicateKeySql,
		Connection: &rconfig.Connection{
			Driver: c.Connection.Driver,
			Dsn:    c.Connection.Dsn,
		},
		BatchSize: c.BatchSize,
	}

	return lConfig, err
}

func detectProduct(ctx context.Context, conn *rconfig.Connection) (product *database.Product, err error) {
	db, err := conn.OpenDB(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	metaService := metadata.New()
	product, err = metaService.DetectProduct(ctx, db)
	if err != nil {
		return nil, err
	}
	return product, nil
}

func ensureTypeDef(rType reflect.Type) string {
	aType := xreflect.NewType(rType.Name(), xreflect.WithReflectType(rType))
	return aType.Body()
}

func ensureConfig(c *config.Config, name string, typeDef string) (*config.Config, *lconfig.Config, error) {
	aConfig := c.PrepareMergeFsConfig()
	aConfig.ExpandConfig(name, typeDef)

	lConfig, err := ensureLoaderConfig(aConfig)
	if err != nil {
		return nil, nil, err
	}

	return aConfig, lConfig, nil
}

func ensureLoader(c *lconfig.Config) (*load.Service, error) {
	if c == nil {
		return nil, nil
	}
	return load.New(c, nil)
}

func checkTableExistence(db *sql.DB, tableName string) (err error) {
	querySQL := fmt.Sprintf("SELECT * FROM %v WHERE 1 = 0", tableName)
	reader, err := read.New(context.Background(), db, querySQL, func() interface{} { return &domain.JournalFs{} })
	if err != nil {
		return err
	}

	defer func() {
		if stmt := reader.Stmt(); stmt != nil {
			err = errors.Join(err, stmt.Close())
		}
	}()

	var journals []*domain.JournalFs
	err = reader.QueryAll(context.Background(), func(row interface{}) error {
		journal := row.(*domain.JournalFs)
		journals = append(journals, journal)
		return nil
	})

	return err
}
