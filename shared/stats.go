package shared

import (
	"database/sql"
	"fmt"
)

func DbStats(db *sql.DB, prefix string) {
	if db == nil {
		return
	}
	dbStats := db.Stats()
	fmt.Printf("%sInUse: %d, Idle: %d, WaitCount: %d\n", prefix, dbStats.InUse, dbStats.Idle, dbStats.WaitCount)
}
