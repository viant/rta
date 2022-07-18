package merge

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/gmetric"
	"github.com/viant/gmetric/provider"
	"github.com/viant/gmetric/stat"
	"github.com/viant/rta/domain"
	"github.com/viant/rta/merge/config"
	"github.com/viant/rta/merge/handler"
	"github.com/viant/rta/shared"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/update"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/product/ansi"
	_ "github.com/viant/sqlx/metadata/product/mysql/load"
	"github.com/viant/sqlx/metadata/registry"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Service struct {
	config  *config.Config
	metrics *gmetric.Service
	counter *gmetric.Operation
	journal *gmetric.Operation
}

func (s *Service) MergeInBackground() {
	timeout := s.config.Timeout()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		startTime := time.Now()
		err := s.Merge(ctx)
		if err != nil {
			log.Printf("failed to merge: %v", err)
		}
		elapsed := time.Now().Sub(startTime)
		thinkTime := s.config.ThinkTime() - elapsed
		if thinkTime > 0 {
			time.Sleep(thinkTime)
		}
		cancel()
	}
}

func (s *Service) Merge(ctx context.Context) error {
	stats := stat.New()
	onDone := s.counter.Begin(time.Now())
	defer func() {
		onDone(time.Now(), stats)
	}()
	db, err := s.config.Connection.OpenDB(ctx)
	if err != nil {
		stats.Append(err)
		return err
	}
	defer db.Close()

	journals, err := s.readFromJournalTable(ctx, db)
	if err != nil {
		stats.Append(err)
		return err
	}
	metaService := metadata.New()
	product, _ := metaService.DetectProduct(ctx, db)

	if product == nil {
		product = &ansi.ANSI
	}
	for _, journal := range journals {
		if err = s.processJournal(ctx, journal, db, product); err != nil {
			stats.Append(err)
		}
		continue
	}
	return nil
}

func (s *Service) processJournal(ctx context.Context, jn *domain.Journal, db *sql.DB, product *database.Product) error {
	stats := stat.New()
	onDone := s.journal.Begin(time.Now())
	defer func() {
		onDone(time.Now(), stats)
	}()
	tx, err := db.Begin()
	if err != nil {
		stats.Append(err)
		return err
	}
	if err = s.mergeToDestTable(ctx, jn, tx, product); err != nil {
		_ = tx.Rollback()
		stats.Append(err)
		return err
	}

	if err = s.updateJournal(ctx, db, jn, tx); err != nil {
		_ = tx.Rollback()
		stats.Append(err)
		return err
	}

	if err = tx.Commit(); err != nil {
		stats.Append(err)
		return err
	}

	if _, err = db.ExecContext(ctx, "DROP TABLE "+jn.TempTableName); err != nil {
		_ = tx.Rollback()
		stats.Append(err)
		return err
	}

	return nil
}

func (s *Service) mergeToDestTable(ctx context.Context, jn *domain.Journal, tx *sql.Tx, product *database.Product) error {
	jnTable := jn.TempTableName
	insertSQL, updateSQL := s.SQL(jnTable, s.config.Dest, product)
	if updateSQL != "" {
		_, err := tx.ExecContext(ctx, updateSQL)
		if err != nil {
			return fmt.Errorf("failed to update: %v, %w", updateSQL, err)
		}
	}
	_, err := tx.ExecContext(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("failed to insert: %v, %w", insertSQL, err)
	}
	return err
}

func (s *Service) readFromJournalTable(ctx context.Context, db *sql.DB) ([]*domain.Journal, error) {
	querySQL := fmt.Sprintf("SELECT * FROM %v WHERE STATUS = %v", s.config.JournalTable, shared.Active)
	reader, err := read.New(ctx, db, querySQL, func() interface{} { return &domain.Journal{} })
	if err != nil {
		return nil, err
	}
	var journals []*domain.Journal
	err = reader.QueryAll(ctx, func(row interface{}) error {
		journal := row.(*domain.Journal)
		journals = append(journals, journal)
		return nil
	})
	return journals, err
}

func (s *Service) updateJournal(ctx context.Context, db *sql.DB, jn *domain.Journal, tx *sql.Tx) error {
	jnTable := s.config.JournalTable
	updater, err := update.New(ctx, db, jnTable)
	if err != nil {
		return err
	}
	jn.Status = shared.InActive
	ts := time.Now()
	jn.Updated = &ts
	_, err = updater.Exec(ctx, jn, tx)
	return err
}

func (s *Service) SQL(source, dest string, product *database.Product) (string, string) {
	columns := strings.Join(append(append(append(s.config.Merge.Others, s.config.Merge.AggregableMax...), s.config.Merge.AggregableSum...), s.config.Merge.UniqueKeys...), ",")
	aggregableSums := ""
	if len(s.config.Merge.AggregableSum) > 0 {
		for _, a := range s.config.Merge.AggregableSum {
			if len(aggregableSums) > 0 {
				aggregableSums = aggregableSums + "," + fmt.Sprintf("%v = %v.%v + s.%v", a, dest, a, a)
			} else {
				aggregableSums = fmt.Sprintf("%v = %v.%v + s.%v", a, dest, a, a)
			}
		}
	}

	aggregableMaxs := ""
	if len(s.config.Merge.AggregableMax) > 0 {
		for _, m := range s.config.Merge.AggregableMax {
			if len(aggregableMaxs) > 0 {
				aggregableMaxs = aggregableMaxs + "," + fmt.Sprintf("%v = (CASE WHEN %v.%v < s.%v THEN s.%v ELSE %v.%v END) ", m, dest, m, m, m, dest, m)
			} else {
				aggregableMaxs = fmt.Sprintf("%v = (CASE WHEN %v.%v < s.%v THEN s.%v ELSE %v.%v END) ", m, dest, m, m, m, dest, m)
			}
		}
	}

	productDialect := registry.LookupDialect(product)
	switch productDialect.Upsert {
	case dialect.UpsertTypeInsertOrUpdate:
		{
			return insertOrUpdateDDL(source, dest, columns, aggregableSums, aggregableMaxs), ""
		}

	case dialect.UpsertTypeInsertOrReplace:
		{

			return insertOrReplaceDDL(source, dest, columns, aggregableSums, aggregableMaxs, s.config.Merge.UniqueKeys)
		}
	default:
		{
			return insertOrReplaceDDL(source, dest, columns, aggregableSums, aggregableMaxs, s.config.Merge.UniqueKeys)
		}
	}

}

func insertOrReplaceDDL(source string, dest string, columns string, aggregableSums string, aggregableMaxs string, uniqueKeys []string) (string, string) {
	uniqueKeysCondition := ""
	for _, key := range uniqueKeys {

		if len(uniqueKeysCondition) > 0 {
			uniqueKeysCondition = uniqueKeysCondition + " AND " + fmt.Sprintf("%v.%v=s.%v", dest, key, key)
		} else {
			uniqueKeysCondition = fmt.Sprintf("%v.%v=s.%v", dest, key, key)
		}
	}

	insertDDL := fmt.Sprintf("INSERT  INTO %v (%v) SELECT %v FROM %v s WHERE NOT EXISTS (SELECT 1 FROM %v  WHERE  %v)", dest, columns, columns, source, dest, uniqueKeysCondition)
	updateDDL := ""
	if aggregableSums != "" || aggregableMaxs != "" {

		if aggregableSums != "" {
			updateDDL = fmt.Sprintf("UPDATE %v  SET %v ", dest, aggregableSums)
			if aggregableMaxs != "" {
				updateDDL = updateDDL + fmt.Sprintf(",%v", aggregableMaxs)
			}
		} else {
			if aggregableMaxs != "" {
				updateDDL = updateDDL + fmt.Sprintf("%v", aggregableMaxs)
			}
		}
		updateDDL = updateDDL + fmt.Sprintf(" FROM %v s WHERE %v  ", source, uniqueKeysCondition)

	}
	return insertDDL, updateDDL
}

func insertOrUpdateDDL(source string, dest string, columns string, aggregableSums string, aggregableMaxs string) string {
	updateDDL := fmt.Sprintf("INSERT INTO %v (%v) SELECT %v FROM %v s", dest, columns, columns, source)
	if aggregableSums != "" || aggregableMaxs != "" {
		updateDDL = updateDDL + " ON DUPLICATE KEY UPDATE "
		if aggregableSums != "" {
			updateDDL = updateDDL + fmt.Sprintf("%v", aggregableSums)
			if aggregableMaxs != "" {
				updateDDL = updateDDL + fmt.Sprintf(",%v", aggregableMaxs)
			}
		} else {
			if aggregableMaxs != "" {
				updateDDL = updateDDL + fmt.Sprintf("%v", aggregableMaxs)
			}
		}

	}
	return updateDDL
}

const (
	metricURI = "/v1/api/metric/"
)

func (s *Service) startEndpoint() {
	if s.config.Endpoint == nil {
		return
	}
	metricHandler := gmetric.NewHandler(metricURI, s.metrics)
	http.Handle(metricURI, metricHandler)
	http.Handle(handler.ConfigURI, handler.NewHandler(s.config))
	http.HandleFunc(handler.StatusURI, handler.StatusOK)
	err := http.ListenAndServe(":"+strconv.Itoa(s.config.Endpoint.Port), http.DefaultServeMux)
	if err != nil {
		log.Fatal(err)
	}
}

func New(c *config.Config) (*Service, error) {
	srv := &Service{config: c, metrics: gmetric.New()}
	srv.counter = srv.metrics.MultiOperationCounter(reflect.TypeOf(srv).PkgPath(), "merge", "merge operation", time.Microsecond, time.Minute, 2, provider.NewBasic())
	srv.journal = srv.metrics.MultiOperationCounter(reflect.TypeOf(srv).PkgPath(), "journal", "journal merge operation", time.Microsecond, time.Minute, 2, provider.NewBasic())
	go srv.startEndpoint()
	return srv, nil
}
