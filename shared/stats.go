package shared

import (
	"database/sql"
	"fmt"
)

func DbStats(db *sql.DB, prefix string) {
	dbStats := db.Stats()
	if dbStats.InUse > 0 {
		fmt.Printf("%sInUse: %d, Idle: %d, WaitCount: %d\n", prefix, dbStats.InUse, dbStats.Idle, dbStats.WaitCount)
	}
}
