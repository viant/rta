package domain

import "time"

type Journal struct {
	ID            int        `sqlx:"name=ID,primaryKey=true,generator=autoincrement"`
	Ip            string     `sqlx:"IP"`
	BatchID       string     `sqlx:"BATCH_ID"`
	Status        int        `sqlx:"STATUS"`
	TempTableName string     `sqlx:"TEMP_TABLE_NAME"`
	Created       *time.Time `sqlx:"CREATED"`
	Updated       *time.Time `sqlx:"UPDATED"`
}
