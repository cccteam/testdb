package dbinitializer

import (
	"github.com/go-playground/errors/v5"
	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresDB represents a container based database
type PostgresDB struct {
	*pgxpool.Pool
	dbName  string
	schema  string
	connstr string
}

// Schema returns the default schema
func (db *PostgresDB) Schema() string {
	return db.schema
}

// MigrateUp will migrate all the way up, applying all up migrations from all sourceURL's
func (db *PostgresDB) MigrateUp(sourceURL ...string) error {

	for _, source := range sourceURL {
		m, err := migrate.New(source, db.connstr)
		if err != nil {
			return errors.Wrapf(err, "migrate.New(): fileURL=%s and connectionURL=%s", source, db.connstr)
		}

		if _, _, err := m.Version(); err == nil {
			if err := m.Force(-1); err != nil {
				return errors.Wrapf(err, "migrate.Migrate.Force(): %s", source)
			}
		}

		if err := m.Up(); err != nil {
			return errors.Wrapf(err, "migrate.Migrate.Up(): %s", source)
		}

		if err, dbErr := m.Close(); err != nil {
			return errors.Wrapf(err, "migrate.Migrate.Close(): source error: %s", source)
		} else if dbErr != nil {
			return errors.Wrapf(dbErr, "migrate.Migrate.Close(): database error: %s", source)
		}
	}

	return nil
}

// MigrateDown will migrate all the way down
func (db *PostgresDB) MigrateDown(sourceURL string) error {

	m, err := migrate.New(sourceURL, db.connstr)
	if err != nil {
		return errors.Wrapf(err, "failed to create new migrate with fileURL=%s and connectionURL=%s", sourceURL, db.connstr)
	}

	if err := m.Down(); err != nil {
		return errors.Wrap(err, "migrate.Migrate.Down()")
	}

	if err, dbErr := m.Close(); err != nil {
		return errors.Wrap(err, "migrate.Migrate.Close(): source error")
	} else if dbErr != nil {
		return errors.Wrap(dbErr, "migrate.Migrate.Close(): database error")
	}

	return nil
}

// Close closes the database connection
func (db *PostgresDB) Close() {
	db.Pool.Close()
}
