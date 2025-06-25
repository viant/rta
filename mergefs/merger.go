package mergefs

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"github.com/viant/gmetric"
	"github.com/viant/gmetric/stat"
	"github.com/viant/rta/collector"
	"github.com/viant/rta/collector/loader"
	"github.com/viant/rta/domain"
	"github.com/viant/rta/mergefs/config"
	"github.com/viant/rta/shared"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/update"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/x"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	metricURI = "/v1/api/metric/"
	alias     = "new"
	logPrefix = config.LogPrefix
	extraTime = 5 * time.Second
)

// Service represents a merger service
type Service struct {
	config       *config.Config
	metrics      *gmetric.Service
	counter      *gmetric.Operation
	journal      *gmetric.Operation
	fs           afs.Service
	xType        *x.Type
	loader       loader.Loader
	dbJn         *sql.DB
	collectorSrv *collector.Service
}

// MergeInBackground merges in background
func (s *Service) MergeInBackground(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		startTime := time.Now()
		errorSlc := s.Merge(context.Background())
		for _, err := range errorSlc {
			log.Printf("%s failed to merge table %s due to: %v", logPrefix, s.config.Dest, err)
		}
		elapsed := time.Since(startTime)
		thinkTime := s.config.ThinkTime() - elapsed
		if thinkTime > 0 {
			time.Sleep(thinkTime)
		}
	}
}

func (s *Service) processJournal(ctx context.Context, parentCtx context.Context, journal *domain.JournalFs, db *sql.DB) (err error) {
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

	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) < extraTime {
			extraCtx, cancel := context.WithTimeout(parentCtx, extraTime)
			ctx = extraCtx
			defer cancel()
		}
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
		return nil, fmt.Errorf("open file error with data from %v due to: %w\n", journal.TempLocation, err)
	}
	defer func() { err = errors.Join(err, reader.Close()) }()

	scanner := bufio.NewScanner(reader)
	adjustScannerBuffer(scanner, s.config.ScannerBufferMB)
	result = []interface{}{}

	for scanner.Scan() {
		// Check if the context was canceled or deadline exceeded
		select {
		case <-ctx.Done():
			ctxErr := ctx.Err()
			stats.Append(ctxErr)
			return nil, fmt.Errorf("context canceled/timeout error while scanning %v: %w\n", journal.TempLocation, ctxErr)
		default:
			// continue with scanning
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		rowPtr := reflect.New(s.xType.Type).Interface()
		if err := gojay.Unmarshal(line, rowPtr); err != nil {
			// workaround for the case with valid json and interface type for Value key:
			// {"ID":1, "Value":"abc\\","Count":1}
			err2 := json.Unmarshal(line, rowPtr)
			if err2 != nil {
				err = errors.Join(err, err2)
				stats.Append(err)
				var logLine string
				maxLen := 500
				if len(line) > maxLen {
					logLine = string(line[:maxLen]) + " (warning: log truncated!)"
				} else {
					logLine = string(line)
				}

				return nil, fmt.Errorf("unmarshal error with data [%s] from %v due to: %w\n", logLine, journal.TempLocation, err)
			}
		}
		result = append(result, rowPtr)
	}

	if err = scanner.Err(); err != nil {
		stats.Append(err)
		return nil, fmt.Errorf("scanner error with data from %v due: %w\n", journal.TempLocation, err)
	}

	return result, nil
}

func (s *Service) load(ctx context.Context, journal *domain.JournalFs, result []interface{}, stats *stat.Values) error {
	if err := s.loader.Load(ctx, result, ""); err != nil {
		stats.Append(err)
		return fmt.Errorf("loader error with data from %v due to: %w\n", journal.TempLocation, err)
	}

	if s.config.Debug {
		fmt.Printf("%s successfully merged table %v with data from %v\n", logPrefix, s.config.Dest, journal.TempLocation)
	}

	return nil
}

func (s *Service) deleteFile(ctx context.Context, url string, stats *stat.Values) error {
	if err := s.fs.Delete(ctx, url); err != nil {
		stats.Append(err)
		return fmt.Errorf("%s failed to delete file %v due to: %w\n", logPrefix, url, err)
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
		return fmt.Errorf("%s failed to update table %v (tx error) after successfully merging data from %v due to: %w\n", logPrefix, s.config.JournalTable, jn.TempLocation, err)
	}

	if err = s.updateJournal(ctx, db, jn, tx); err != nil {
		err = errors.Join(err, tx.Rollback())
		stats.Append(err)
		return fmt.Errorf("%s failed to update table %v after successfully merging data from %v due to: %w\n", logPrefix, s.config.JournalTable, jn.TempLocation, err)
	}

	if s.config.Debug {
		fmt.Printf("%s successfully updated table %v\n", logPrefix, s.config.JournalTable)
	}

	if err = tx.Commit(); err != nil {
		stats.Append(err)
		return fmt.Errorf("%s failed to commit table %v after successfully merging data from %v due to: %w\n", logPrefix, s.config.JournalTable, jn.TempLocation, err)
	}

	if s.config.Debug {
		fmt.Printf("%s successfully committed table %v\n", logPrefix, s.config.JournalTable)
	}

	return nil
}

func (s *Service) updateJnSlc(ctx context.Context, jn []*domain.JournalFs, db *sql.DB, stats *stat.Values) error {
	tx, err := db.Begin()
	if err != nil {
		stats.Append(err)
		return fmt.Errorf("%s failed to update table %v (tx error) after successfully merging data from %v due to: %w\n", logPrefix, s.config.JournalTable, getLocations(jn), err)
	}

	if err = s.updateJournals(ctx, db, jn, tx); err != nil {
		err = errors.Join(err, tx.Rollback())
		stats.Append(err)
		return fmt.Errorf("%s failed to update table %v after successfully merging data from %v due to: %w\n", logPrefix, s.config.JournalTable, getLocations(jn), err)
	}

	if s.config.Debug {
		fmt.Printf("%s successfully updated table %v\n", logPrefix, s.config.JournalTable)
	}

	if err = tx.Commit(); err != nil {
		stats.Append(err)
		return fmt.Errorf("%s failed to commit table %v after successfully merging data from %v due to: %w\n", logPrefix, s.config.JournalTable, getLocations(jn), err)
	}

	if s.config.Debug {
		fmt.Printf("%s successfully committed table %v\n", logPrefix, s.config.JournalTable)
	}

	return nil
}

func getLocations(jn []*domain.JournalFs) string {
	// Gather all TempLocations in a single string
	var locations []string
	for _, journal := range jn {
		locations = append(locations, journal.TempLocation)
	}
	return strings.Join(locations, ", ")
}

func (s *Service) readFromJournalTable(ctx context.Context, db *sql.DB) (journals []*domain.JournalFs, err error) {
	querySQL := fmt.Sprintf("SELECT * FROM %v WHERE STATUS = %v ORDER BY CREATED", s.config.JournalTable, shared.Active)
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

func (s *Service) updateJournals(ctx context.Context, db *sql.DB, jn []*domain.JournalFs, tx *sql.Tx) error {
	jnTable := s.config.JournalTable
	updater, err := update.New(ctx, db, jnTable)
	if err != nil {
		return err
	}
	for i := range jn {
		jn[i].Status = shared.InActive
		ts := time.Now()
		jn[i].Updated = &ts
	}

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

func (s *Service) Merge(ctx context.Context) []error {
	timeout := s.config.Timeout()
	stats := stat.New()
	onDone := s.counter.Begin(time.Now())
	defer func() {
		onDone(time.Now(), stats)
	}()

	errorSlc := make([]error, 0)

	readCtx, cancel := context.WithTimeout(ctx, timeout)
	journals, err := s.readFromJournalTable(readCtx, s.dbJn)
	if err != nil {
		cancel()
		stats.Append(err)
		return append(errorSlc, err)
	}
	cancel() // readCtx only needed for readFromJournalTable

	if len(journals) == 0 {
		if s.config.Debug {
			fmt.Printf("%s no journals to process for %s\n", logPrefix, s.config.JournalTable)
		}
		return nil
	}

	if s.config.Collector != nil {
		errorSlc = s.mergeJournalsWithCollector(ctx, journals, timeout, stats)
	} else {
		errorSlc = s.mergeJournals(ctx, journals, timeout, stats)
	}

	return errorSlc
}

func (s *Service) mergeJournals(ctx context.Context, journals []*domain.JournalFs, timeout time.Duration, stats *stat.Values) []error {
	var err error
	errorSlc := make([]error, 0)

	// Process each journal with its own context and optional think time
	for _, journal := range journals {
		journalCtx, procCancel := context.WithTimeout(ctx, timeout)
		startTime := time.Now()
		// don't collect all errors, return first maxErrLogCnt
		if err = s.processJournal(journalCtx, ctx, journal, s.dbJn); err != nil {
			stats.Append(err)
			errorSlc = append(errorSlc, err)
		}
		elapsed := time.Since(startTime)
		thinkTime := s.config.ThinkTimeJournal() - elapsed
		if thinkTime > 0 {
			time.Sleep(thinkTime)
		}
		procCancel()
	}

	return errorSlc
}

func (s *Service) mergeJournalsWithCollector(ctx context.Context, journals []*domain.JournalFs, timeout time.Duration, stats *stat.Values) []error {
	jnCnt := len(journals)
	if jnCnt == 0 {
		return nil
	}

	chunkSize := s.config.MaxJournalsInChunk
	if chunkSize <= 0 {
		chunkSize = 2
	}
	partCnt := jnCnt / chunkSize

	if jnCnt%chunkSize != 0 {
		partCnt++
	}

	errorSlc := make([]error, 0)

	// don't use concurrency, process serially in chunks - prevent mixing data in one batch collector from different chunks
	for n := 0; n < partCnt; n++ {
		begin := n * chunkSize
		end := (n + 1) * chunkSize
		if n == partCnt-1 { // the last part
			end = jnCnt
		}

		journalCtx, cancel := context.WithTimeout(ctx, timeout)
		startTime := time.Now()
		if err := s.processJournalsWithCollector(journalCtx, ctx, journals[begin:end], s.dbJn); err != nil {
			stats.Append(err)
			errorSlc = append(errorSlc, err)
		}
		elapsed := time.Since(startTime)
		thinkTime := s.config.ThinkTimeJournal() - elapsed
		if thinkTime > 0 {
			time.Sleep(thinkTime)
		}
		cancel()
	}

	return errorSlc
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

func (s *Service) processJournalsWithCollector(ctx context.Context, parentCtx context.Context, journals []*domain.JournalFs, db *sql.DB) (err error) {
	stats := stat.New()
	onDone := s.journal.Begin(time.Now())
	defer func() {
		onDone(time.Now(), stats)
	}()

	start := time.Now()

	data, corrupted, corruptedCnt, err := s.fetchJournalsData(ctx, journals, stats)
	if err != nil {
		if corruptedCnt == len(journals) {
			return err
		}

		fmt.Printf("%s %d/%d journals are corrupted, but we can continue with the rest despite errors: %v\n", logPrefix, corruptedCnt, len(journals), err)
		err = nil
		resultJn := make([]*domain.JournalFs, 0, len(journals))
		resultData := make([][]interface{}, 0, len(journals))
		for i, invalid := range corrupted {
			if invalid {
				continue
			}
			resultJn = append(resultJn, journals[i])
			resultData = append(resultData, data[i])
		}
		journals = resultJn
		data = resultData
	}

	// TODO load just with collector loader in case there's only 1 batch as a option
	err = s.loadWithCollector(ctx, data, &start)
	if err != nil {
		return err
	}

	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) < extraTime {
			extraCtx, cancel := context.WithTimeout(parentCtx, extraTime)
			ctx = extraCtx
			defer cancel()
		}
	}

	err = s.updateJnSlc(ctx, journals, db, stats)
	if err != nil {
		return err
	}

	err = s.deleteFiles(ctx, journals, stats)
	if err != nil {
		return err
	}

	return err
}

func (s *Service) deleteFiles(ctx context.Context, journals []*domain.JournalFs, stats *stat.Values) error {
	var err error
	for _, jn := range journals {
		err2 := s.deleteFile(ctx, jn.TempLocation, stats)
		if err2 != nil {
			err = errors.Join(err, err2)
		}
	}

	return err
}

func (s *Service) loadWithCollector(ctx context.Context, results [][]interface{}, start *time.Time) error {
	for i, r := range results {
		if r == nil {
			return fmt.Errorf("loadWithCollector - result is nil for at index %d", i)
		}
	}

	for _, r := range results {
		err := s.collectorSrv.CollectAll(r...)
		if err != nil {
			return fmt.Errorf("loadWithCollector - unable to collect data due to: %w", err)
		}
	}

	b, err := s.collectorSrv.GetBatch()
	if err != nil {
		return fmt.Errorf("loadWithCollector - unable to get batch due to: %w", err)
	}

	err = s.collectorSrv.FlushOnDemand(ctx, b, len(results), start)
	if err != nil {
		return fmt.Errorf("loadWithCollector - unable to flush batch due to: %w", err)
	}
	return nil
}

func (s *Service) fetchJournalsData(ctx context.Context, journals []*domain.JournalFs, stats *stat.Values) ([][]interface{}, []bool, int, error) {
	wg := sync.WaitGroup{}
	results := make([][]interface{}, len(journals))
	errorSlc := make([]error, len(journals))
	corrupted := make([]bool, len(journals))
	corruptedCnt := 0

	for i, jn := range journals {
		wg.Add(1)
		go func(i int, jn *domain.JournalFs) {
			defer wg.Done()
			result, err := s.readData(ctx, jn, stats)
			if err != nil { // don't cancel the context, just log the error and process what possible
				stats.Append(err)
				errorSlc[i] = err
				corrupted[i] = true
				corruptedCnt++
				return
			}
			results[i] = result
		}(i, jn)
	}
	wg.Wait()

	err := errors.Join(errorSlc...)

	return results, corrupted, corruptedCnt, err // return results even in case of error
}

func adjustScannerBuffer(scanner *bufio.Scanner, sizeMB int) {
	if sizeMB > 0 {
		scanner.Buffer(make([]byte, 0, 64*1024), sizeMB*1024*1024)
	}
}
