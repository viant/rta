package load

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/rta/domain"
	"github.com/viant/rta/load/config"
	"github.com/viant/rta/shared"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/load"
	"github.com/viant/sqlx/io/read"
	_ "github.com/viant/sqlx/metadata/product/mysql/load"
	"github.com/viant/sqlx/option"
	"os"
	"strings"
	"time"
)

type Service struct {
	config       *config.Config
	hostIP       string
	suffixHostIp string
	suffix       config.Suffix
}

func (s *Service) Load(ctx context.Context, data interface{}, batchID string) error {
	db, err := s.config.Connection.OpenDB(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.Begin()
	recordExist, tempTable, err := s.loadToTempTable(ctx, data, db, tx, batchID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if recordExist {
		return nil
	}
	err = s.insertToJournal(ctx, db, tempTable, tx, batchID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

type Exist struct {
	X int
}

func (s *Service) checkRecordExist(ctx context.Context, db *sql.DB, batchID string) (bool, error) {
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
	return count > 0, err
}

func (s *Service) loadToTempTable(ctx context.Context, data interface{}, db *sql.DB, tx *sql.Tx, batchID string) (bool, string, error) {
	exist, err := s.checkRecordExist(ctx, db, batchID)
	if err != nil {
		return false, "", err
	}
	if exist {
		return true, "", nil
	}
	sourceTable := s.config.TransientTable() + "_" + s.suffixHostIp + "_" + s.config.Suffix()()
	DDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v AS SELECT * FROM %v WHERE 1=0", sourceTable, s.config.Dest)
	if _, err := db.Exec(DDL); err != nil {
		return false, "", err
	}
	loadFn, err := s.loadFn(ctx, db, sourceTable)
	if err != nil {
		return false, "", err
	}
	_, err = loadFn(ctx, data, tx)
	return false, sourceTable, err
}

func (s *Service) loadFn(ctx context.Context, db *sql.DB, sourceTable string) (Load, error) {
	if s.config.UseInsertAPI {
		srv, err := insert.New(ctx, db, sourceTable)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, any interface{}, options ...option.Option) (int, error) {
			affected, _, err := srv.Exec(ctx, any, options...)
			return int(affected), err
		}, nil
	}
	srv, err := load.New(ctx, db, sourceTable)
	if err != nil {
		return nil, err
	}
	return srv.Exec, nil
}

func (s *Service) insertToJournal(ctx context.Context, db *sql.DB, tempTable string, tx *sql.Tx, batchID string) error {
	jnTable := s.config.JournalTable
	insert, err := insert.New(ctx, db, jnTable)
	if err != nil {
		return err
	}
	ts := time.Now()
	journal := &domain.Journal{
		Ip:            s.hostIP,
		BatchID:       batchID,
		Status:        shared.Active,
		TempTableName: tempTable,
		Created:       &ts,
	}
	_, _, err = insert.Exec(ctx, journal, tx)
	return err

}

func (s *Service) init() error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	s.hostIP, err = shared.GetHostIPv4(hostname)
	if err != nil && strings.Contains(err.Error(), "no such host") {
		s.hostIP, err = shared.GetHostIPv4("localhost")
		if err != nil {
			return err
		}
	}
	if s.hostIP == "::1" {
		s.hostIP = "127.0.0.1"
		s.suffixHostIp = "localhost"
	} else {
		s.suffixHostIp = strings.Replace(s.hostIP, ".", "", -1)
	}
	return nil

}

func New(c *config.Config, suffix config.Suffix) (*Service, error) {
	if suffix != nil {
		c.SetSuffix(suffix)
	}
	srv := &Service{config: c}
	return srv, srv.init()

}
