package metrics

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/src-d/gitcollector/library"

	// postgres database driver
	_ "github.com/lib/pq"
)

// PrepareDB performs the necessary operations to send metrics to a postgres
// database.
func PrepareDB(uri string, table, org string) (*sql.DB, error) {
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	statements := []string{
		fmt.Sprintf(create, table),
		fmt.Sprintf(addColumns, table),
		fmt.Sprintf(insert, table, org),
	}

	tx, err := db.Begin()
	if err != nil {
		db.Close()
		return nil, err
	}

	for _, s := range statements {
		if _, err := tx.Exec(s); err != nil {
			tx.Rollback()
			db.Close()
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

const (
	create = `CREATE TABLE IF NOT EXISTS %s (
		org VARCHAR(50) NOT NULL,
		discovered INTEGER NOT NULL,
		downloaded INTEGER NOT NULL,
		updated INTEGER NOT NULL,
		failed INTEGER NOT NULL
	)`

	insert = `INSERT INTO %[1]s(org, discovered, downloaded, updated, failed)
	SELECT '%[2]s',0,0,0,0
	WHERE NOT EXISTS (SELECT * FROM %[1]s)`

	addColumns = `ALTER TABLE %s
	ADD COLUMN IF NOT EXISTS discovered INTEGER,
	ADD COLUMN IF NOT EXISTS downloaded INTEGER,
	ADD COLUMN IF NOT EXISTS updated INTEGER,
	ADD COLUMN IF NOT EXISTS failed INTEGER`

	update = `UPDATE %s
	SET discovered = %d,
	    downloaded = %d,
	    updated = %d,
	    failed = %d
	WHERE org = '%s';`
)

// SendToDB is a SendFn to persist metrics on a database.
func SendToDB(db *sql.DB, table, org string) SendFn {
	return func(
		_ context.Context,
		mc *Collector,
		_ *library.Job,
	) error {
		statement := fmt.Sprintf(
			update,
			table,
			mc.discoverCount,
			mc.successDownloadCount,
			mc.successUpdateCount,
			mc.failCount,
			org,
		)

		_, err := db.Exec(statement)
		return err
	}
}
