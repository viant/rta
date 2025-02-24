package mergefs

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"github.com/viant/gmetric"
	"github.com/viant/gmetric/stat"
	"github.com/viant/rta/collector/loader"
	"github.com/viant/rta/domain"
	"github.com/viant/rta/mergefs/config"
	"github.com/viant/rta/shared"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/update"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info/dialect"
	_ "github.com/viant/sqlx/metadata/product/mysql/load"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/x"
	"log"
	"reflect"
	"sync"
	"time"
)

const (
	metricURI = "/v1/api/metric/"
	alias     = "new"
	logPrefix = config.LogPrefix
)

// Service represents a merger service
type Service struct {
	config  *config.Config
	metrics *gmetric.Service
	counter *gmetric.Operation
	journal *gmetric.Operation
	fs      afs.Service
	xType   *x.Type
	loader  loader.Loader
	dbJn    *sql.DB
}

// MergeInBackground merges in background
func (s *Service) MergeInBackground(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		startTime := time.Now()
		err := s.Merge(context.Background())
		if err != nil {
			log.Printf("%s failed to merge: %v", logPrefix, err)
		}
		elapsed := time.Since(startTime)
		thinkTime := s.config.ThinkTime() - elapsed
		if thinkTime > 0 {
			time.Sleep(thinkTime)
		}
	}
}

func (s *Service) processJournal(ctx context.Context, journal *domain.JournalFs, db *sql.DB) (err error) {
	stats := stat.New()
	onDone := s.journal.Begin(time.Now())
	defer func() {
		onDone(time.Now(), stats)
	}()

	result, err := s.readData(ctx, journal, stats)
	if err != nil {
		return err
	}

	err = s.load(ctx, journal, result, stats)
	if err != nil {
		return err
	}

	err = s.updateJn(ctx, journal, db, stats)
	if err != nil {
		return err
	}

	err = s.deleteFile(ctx, journal.TempLocation, stats)
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) readData(ctx context.Context, journal *domain.JournalFs, stats *stat.Values) (result []interface{}, err error) {
	reader, err := s.fs.OpenURL(ctx, journal.TempLocation)
	if err != nil {
		stats.Append(err)
		if s.config.Debug {
			fmt.Printf("%s failed to merge table %v (open file error) with data from %v due to: %v\n", logPrefix, s.config.Dest, journal.TempLocation, err)
		}
		return nil, err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()

	scanner := bufio.NewScanner(reader)
	result = []interface{}{}

	for scanner.Scan() {
		// Check if the context was canceled or deadline exceeded
		select {
		case <-ctx.Done():
			ctxErr := ctx.Err()
			stats.Append(ctxErr)
			if s.config.Debug {
				fmt.Printf("%s failed to merge table %v (context canceled/timeout) while scanning %v: %v\n", logPrefix, s.config.Dest, journal.TempLocation, ctxErr)
			}
			return nil, ctxErr
		default:
			// continue with scanning
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		rowPtr := reflect.New(s.xType.Type).Interface()
		if err := gojay.Unmarshal(line, rowPtr); err != nil {
			stats.Append(err)
			if s.config.Debug {
				fmt.Printf("%s failed to merge table %v (unmarshal error) with data from %v due to: %v\n", logPrefix, s.config.Dest, journal.TempLocation, err)
			}
			return nil, err
		}
		result = append(result, rowPtr)
	}

	if err = scanner.Err(); err != nil {
		stats.Append(err)
		if s.config.Debug {
			fmt.Printf("%s failed to merge table %v (scanner error) with data from %v due: %v\n", logPrefix, s.config.Dest, journal.TempLocation, err)
		}
		return nil, err
	}

	return result, nil
}

func (s *Service) load(ctx context.Context, journal *domain.JournalFs, result []interface{}, stats *stat.Values) error {
	if err := s.loader.Load(ctx, result, ""); err != nil {
		stats.Append(err)
		if s.config.Debug {
			fmt.Printf("%s failed to merge table %v (loader error) with data from %v due to: %v\n", logPrefix, s.config.Dest, journal.TempLocation, err)
		}
		return err
	}

	if s.config.Debug {
		fmt.Printf("%s successfully merged table %v with data from %v\n", logPrefix, s.config.Dest, journal.TempLocation)
	}

	return nil
}

func (s *Service) deleteFile(ctx context.Context, url string, stats *stat.Values) error {
	if err := s.fs.Delete(ctx, url); err != nil {
		stats.Append(err)
		if s.config.Debug {
			fmt.Printf("%s failed to delete file %v due to: %v\n", logPrefix, url, err)
		}
		return err
	}

	if s.config.Debug {
		fmt.Printf("%s successfully deleted file %v\n", logPrefix, url)
	}

	return nil
}

func (s *Service) updateJn(ctx context.Context, jn *domain.JournalFs, db *sql.DB, stats *stat.Values) error {
	tx, err := db.Begin()
	if err != nil {
		stats.Append(err)
		if s.config.Debug {
			fmt.Printf("%s failed to update table %v (tx error) after successfully merging data from %v due to: %v\n", logPrefix, s.config.JournalTable, jn.TempLocation, err)
		}
		return err
	}

	if err = s.updateJournal(ctx, db, jn, tx); err != nil {
		_ = tx.Rollback()
		stats.Append(err)
		if s.config.Debug {
			fmt.Printf("%s failed to update table %v after successfully merging data from %v due to: %v\n", logPrefix, s.config.JournalTable, jn.TempLocation, err)
		}
		return err
	}

	if s.config.Debug {
		fmt.Printf("%s successfully updated table %v\n", logPrefix, s.config.JournalTable)
	}

	if err = tx.Commit(); err != nil {
		stats.Append(err)
		if s.config.Debug {
			fmt.Printf("%s failed to commit table %v after successfully merging data from %v due to: %v\n", logPrefix, s.config.JournalTable, jn.TempLocation, err)
		}
		return err
	}

	if s.config.Debug {
		fmt.Printf("%s successfully committed table %v\n", logPrefix, s.config.JournalTable)
	}

	return nil
}

func (s *Service) readFromJournalTable(ctx context.Context, db *sql.DB) (journals []*domain.JournalFs, err error) {
	querySQL := fmt.Sprintf("SELECT * FROM %v WHERE STATUS = %v", s.config.JournalTable, shared.Active)
	reader, err := read.New(ctx, db, querySQL, func() interface{} { return &domain.JournalFs{} })
	if err != nil {
		return nil, err
	}

	defer func() {
		if stmt := reader.Stmt(); stmt != nil {
			err = errors.Join(err, stmt.Close())
		}
	}()

	err = reader.QueryAll(ctx, func(row interface{}) error {
		journal := row.(*domain.JournalFs)
		journals = append(journals, journal)
		return nil
	})

	return journals, err
}

func (s *Service) updateJournal(ctx context.Context, db *sql.DB, jn *domain.JournalFs, tx *sql.Tx) error {
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

func onDuplicateKey(product *database.Product, cfg *config.Config) (string, error) {
	aggregableSums := ""
	for _, a := range cfg.Merge.AggregableSum {
		aggregableSums = appendClause(aggregableSums, fmt.Sprintf("%v = %v + %s.%v", a, a, alias, a))
	}

	aggregableMaxs := ""
	for _, m := range cfg.Merge.AggregableMax {
		aggregableMaxs = appendClause(aggregableMaxs, fmt.Sprintf("%v = (CASE WHEN %v < %s.%v THEN %s.%v ELSE %v END) ", m, m, alias, m, alias, m, m))
	}

	overriddens := ""
	for _, o := range cfg.Merge.Overridden {
		overriddens = appendClause(overriddens, fmt.Sprintf("%v = %s.%v", o, alias, o))
	}

	productDialect := registry.LookupDialect(product)
	switch productDialect.Upsert {
	case dialect.UpsertTypeInsertOrUpdate:
		return updatePart(alias, aggregableSums, aggregableMaxs, overriddens), nil
	default:
		return "", fmt.Errorf("unsupported upsert type: %v", productDialect.Upsert)
	}
}

func updatePart(alias string, aggregableSums string, aggregableMaxs string, overriddens string) string {
	if aggregableSums == "" && aggregableMaxs == "" && overriddens == "" {
		return ""
	}

	updateDDL := " AS " + alias + " ON DUPLICATE KEY UPDATE "
	updateClause := aggregableSums
	updateClause = appendClause(updateClause, aggregableMaxs)
	updateClause = appendClause(updateClause, overriddens)

	return updateDDL + updateClause
}

func (s *Service) Merge(ctx context.Context) (err error) {
	timeout := s.config.Timeout()
	stats := stat.New()
	onDone := s.counter.Begin(time.Now())
	defer func() {
		onDone(time.Now(), stats)
	}()

	readCtx, cancel := context.WithTimeout(ctx, timeout)
	journals, err := s.readFromJournalTable(readCtx, s.dbJn)
	if err != nil {
		stats.Append(err)
		return err
	}
	cancel() // We only needed readCtx for readFromJournalTable

	// Process each journal with its own context and optional think time
	for _, journal := range journals {
		journalCtx, procCancel := context.WithTimeout(ctx, timeout)
		startTime := time.Now()
		if err = s.processJournal(journalCtx, journal, s.dbJn); err != nil {
			stats.Append(err)
		}
		elapsed := time.Since(startTime)
		thinkTime := s.config.ThinkTimeJournal() - elapsed
		if thinkTime > 0 {
			time.Sleep(thinkTime)
		}
		procCancel()
	}

	// We don't need all errors, just return the last one
	return err
}

func appendClause(existing, clause string) string {
	if existing == "" {
		return clause
	}
	if clause == "" {
		return existing
	}
	return existing + ", " + clause
}
