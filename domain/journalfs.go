package domain

import "time"

type JournalFs struct {
	ID           int        `sqlx:"name=ID,primaryKey=true,generator=autoincrement"`
	Ip           string     `sqlx:"IP"`
	BatchID      string     `sqlx:"BATCH_ID"`
	Status       int        `sqlx:"STATUS"`
	TempLocation string     `sqlx:"TEMP_TABLE_NAME"`
	Created      *time.Time `sqlx:"CREATED"`
	Updated      *time.Time `sqlx:"UPDATED"`
}
