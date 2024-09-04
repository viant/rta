package load

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/rta/collector/loader"
	"github.com/viant/rta/domain"
	"github.com/viant/rta/shared"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/load"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/loption"
	"github.com/viant/sqlx/metadata/info/dialect"
	_ "github.com/viant/sqlx/metadata/product/mysql/load"
	"github.com/viant/sqlx/option"
	"strings"
	"time"
)

func (s *Service) loadDirect(ctx context.Context, data interface{}, batchID string, options ...loader.Option) error {
	db, err := s.config.Connection.OpenDB(ctx)
	if err != nil {
		return err
	}
	defer db.Close()

	var dbJn *sql.DB
	if s.config.ConnectionJn != nil {
		dbJn, err = s.config.ConnectionJn.OpenDB(ctx)
		if err != nil {
			return err
		}
		defer dbJn.Close()
	}

	collectorId := loader.NewOptions(options...).GetInstanceId()
	recordExist, tempTable, err := s.loadDirectToTable(ctx, data, db, dbJn, batchID, collectorId)
	if err != nil {
		return err
	}
	if recordExist {
		return nil
	}

	// TODO how to check if data was persisted before committing to journal?
	// TODO how to rollback inserts if journal insert fails

	if dbJn != nil && s.config.JournalTable != "" {
		tx, err := dbJn.Begin()
		if err != nil {
			return err
		}
		err = s.insertToJournalDirect(ctx, dbJn, tempTable, tx, batchID)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to insert into journal table: %w", err)
		}
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit - load/insert into journal table: %w", err)
		}
	}

	return nil
}

func (s *Service) checkRecordExistInJounralDirect(ctx context.Context, db *sql.DB, batchID string) (bool, error) {
	querySQL := fmt.Sprintf("SELECT 1 AS X  FROM %v WHERE BATCH_ID ='%v' AND IP ='%v'", s.config.JournalTable, batchID, s.hostIP)
	reader, err := read.New(ctx, db, querySQL, func() interface{} { return &Exist{} })
	if err != nil {
		return false, err
	}
	count := 0
	err = reader.QueryAll(ctx, func(row interface{}) error {
		count++
		return nil
	})

	stmt := reader.Stmt()
	if stmt != nil {
		err2 := stmt.Close()
		if err == nil && err2 != nil {
			err = err2
		}
	}

	return count > 0, err
}

func (s *Service) loadDirectToTable(ctx context.Context, data interface{}, db *sql.DB, dbJn *sql.DB, batchID string, collectorId string) (bool, string, error) {
	var err error

	if dbJn != nil && s.config.JournalTable != "" {
		exist, err := s.checkRecordExistInJounralDirect(ctx, dbJn, batchID)
		if err != nil {
			return false, "", err
		}
		if exist {
			return true, "", nil
		}
	}
	var sourceTable = s.config.Dest

	DDL := strings.TrimSpace(s.config.CreateDDL)
	DDL = s.expandDDL(DDL)
	if len(DDL) > 0 {
		_, err = db.ExecContext(ctx, DDL)
	}
	if err != nil {
		return false, "", err
	}

	loadFnDirect, err := s.loadFnDirect(ctx, db, sourceTable)
	if err != nil {
		return false, "", fmt.Errorf("failed to get load fn for %v, %w", sourceTable, err)
	}

	_, err = loadFnDirect(ctx, data, WithDb(db))
	if err != nil {
		err = fmt.Errorf("failed to load data into %v, %w", sourceTable, err)
	}
	return false, sourceTable, err
}

func (s *Service) loadFnDirect(ctx context.Context, db *sql.DB, sourceTable string) (Load, error) {
	if s.config.UseInsertAPI {
		srv, err := insert.New(ctx, db, sourceTable, option.OnDuplicateKeySql(s.config.OnDuplicateKeySql))
		if err != nil {
			return nil, fmt.Errorf("failed to create insert service: %w", err)
		}
		return func(ctx context.Context, any interface{}, opts ...Option) (int, error) {
			options := newOptions(opts)
			batchSize := s.config.BatchSize
			if batchSize < 1 {
				batchSize = 1
			}
			affected, _, err := srv.Exec(ctx, any, options.db, option.BatchSize(batchSize))
			return int(affected), err
		}, nil
	}

	// TODO should work only with mysql; not supported in aerospike
	srv, err := load.New(ctx, db, sourceTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create load service: %w", err)
	}
	return func(ctx context.Context, any interface{}, opts ...Option) (int, error) {
		options := newOptions(opts)
		return srv.Exec(ctx, any, loption.WithCommonOptions([]option.Option{options.db}))
	}, nil
}

func (s *Service) insertToJournalDirect(ctx context.Context, db *sql.DB, tempTable string, tx *sql.Tx, batchID string) error {
	jnTable := s.config.JournalTable
	insert, err := insert.New(ctx, db, jnTable)
	if err != nil {
		return err
	}
	ts := time.Now()
	journal := &domain.Journal{
		Ip:            s.hostIP,
		BatchID:       batchID,
		Status:        shared.InActive,
		TempTableName: tempTable,
		Created:       &ts,
	}
	_, _, err = insert.Exec(ctx, journal, tx, dialect.PresetIDStrategyIgnore)
	return err
}

func (s *Service) expandDDL(ddl string) string {
	if index := strings.Index(ddl, "${Dest}"); index != -1 {
		return strings.ReplaceAll(ddl, "${Dest}", s.config.Dest)
	}
	return ddl
}
