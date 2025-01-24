package mergefs

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/gmetric"
	"github.com/viant/gmetric/provider"
	cconfig "github.com/viant/rta/config"
	rconfig "github.com/viant/rta/config"
	"github.com/viant/rta/load"
	lconfig "github.com/viant/rta/load/config"
	"github.com/viant/rta/mergefs/config"
	"github.com/viant/rta/mergefs/handler"
	"github.com/viant/rta/mergefs/registry"
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
	config  *config.Config
	mergers []*Service
	metrics *gmetric.Service
	counter *gmetric.Operation
	journal *gmetric.Operation
	fs      afs.Service
	xType   *x.Type
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
	for _, merger := range m.mergers {
		go merger.MergeInBackground()
	}
}

// Merge run merge function for all mergers
func (m *MultiMerger) Merge(ctx context.Context) error {
	var err error
	var mux sync.Mutex
	wg := sync.WaitGroup{}
	for i, _ := range m.mergers {
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
	}

	r := registry.Registry()
	result.xType = registry.LookUp(r, c.TypeName)
	if result.xType == nil {
		return nil, fmt.Errorf("%s New: failed to lookup type: %v", logPrefix, c.TypeName)
	}
	typeDef := ensureTypeDef(result.xType.Type)

	err := populateMergers(c, result, typeDef)
	if err != nil {
		return nil, fmt.Errorf("%s New: failed to pupulate mergers due to: %w", logPrefix, err)
	}

	go result.startEndpoint()
	return result, nil
}

func populateMergers(c *config.Config, result *MultiMerger, typeDef string) error {
	placeHolders := []string{""}
	if len(c.DestPlaceholders) > 0 {
		placeHolders = c.DestPlaceholders
	}

	for _, name := range placeHolders {
		merger, err := newMerger(c, result.metrics, result.fs, result.xType, name, typeDef)
		if err != nil {
			return fmt.Errorf("failed to create merger for placeholder %q: %w", name, err)
		}
		result.mergers = append(result.mergers, merger)
	}
	return nil
}

func newMerger(c *config.Config, metrics *gmetric.Service, fs afs.Service, xType *x.Type, name, typeDef string) (*Service, error) {
	aConfig, lConfig, err := ensureConfig(c, name, typeDef)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure configuration for %q: %w", name, err)
	}

	if err := lConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid loader configuration for %q: %w", name, err)
	}

	loader, err := ensureLoader(lConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize loader for %q: %w", name, err)
	}

	srv := &Service{
		config:  aConfig,
		metrics: metrics,
		fs:      fs,
		xType:   xType,
		loader:  loader,
	}
	ensureCounters(c, srv, name)

	return srv, nil
}

func ensureCounters(c *config.Config, srv *Service, name string) {
	suffix := ""
	if len(c.DestPlaceholders) > 0 {
		suffix = "_" + strings.ToLower(name)
	}

	srv.counter = srv.metrics.MultiOperationCounter(reflect.TypeOf(srv).PkgPath(), mergeOperation+suffix, mergeOperation+" "+operationSuffix, time.Microsecond, time.Minute, 2, provider.NewBasic())
	srv.journal = srv.metrics.MultiOperationCounter(reflect.TypeOf(srv).PkgPath(), journalOperation+suffix, journalOperation+" "+mergeOperation+" "+operationSuffix, time.Microsecond, time.Minute, 2, provider.NewBasic())
}

func ensureLoaderConfig(c *config.Config) (lConfig *lconfig.Config, err error) {
	ctx := context.Background()
	product, err := detectProduct(ctx, c.JournalConnection)
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
		Connection: &cconfig.Connection{
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
