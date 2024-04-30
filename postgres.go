package dbinitializer

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/docker/go-connections/nat"
	"github.com/go-playground/errors/v5"
	shopspring "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	_ "github.com/golang-migrate/migrate/v4/database/postgres" // database driver for the migrate package
	_ "github.com/golang-migrate/migrate/v4/source/file"       // up/down script file source driver for the migrate package
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultPostgresPort     = "5432"
	defaultPostgresDatabase = "postgres"
)

// PostgresContainer represents a docker container running a postgres instance.
type PostgresContainer struct {
	testcontainers.Container
	host                      string
	port                      nat.Port
	sslMode                   string
	superUserUsername         string
	unpriviledgedUserUsername string
	password                  string
	defaultDatabase           string

	sMu                  sync.Mutex
	superUserConnections map[string]*pgxpool.Pool

	muReplacementCount sync.Mutex
	replacementCount   int
}

// NewPostgresContainer returns a new PostgresContainer ready to use with postgres.
func NewPostgresContainer(ctx context.Context) (*PostgresContainer, error) {
	pg, err := initPostgresContainer(ctx)
	if err != nil {
		return nil, err
	}

	if err := pg.addUnprivilegedUser(ctx); err != nil {
		return nil, err
	}

	return pg, nil
}

// initPostgresContainer returns a PostgresContainer which represents a newly started docker container running postgres.
func initPostgresContainer(ctx context.Context) (*PostgresContainer, error) {
	password := "password"

	req := testcontainers.ContainerRequest{
		Image:        "postgres:latest",
		Cmd:          []string{"postgres", "-c", "max_connections=250"},
		WaitingFor:   wait.ForLog(" UTC [1] LOG:  database system is ready to accept connections"),
		ExposedPorts: []string{defaultPostgresPort},
		Env: map[string]string{
			"POSTGRES_PASSWORD": password,
		},
	}

	postgresC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started:          true,
		ContainerRequest: req,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create container using ContainerRequest=%v", req)
	}

	externalPort, err := postgresC.MappedPort(ctx, nat.Port(defaultPostgresPort))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get external port for exposed port %s", defaultPostgresPort)
	}

	pg := &PostgresContainer{
		Container:                 postgresC,
		host:                      "localhost",
		port:                      externalPort,
		sslMode:                   "disable",
		superUserConnections:      make(map[string]*pgxpool.Pool, 0),
		superUserUsername:         "postgres",
		unpriviledgedUserUsername: "unprivileged",
		password:                  "password",
		defaultDatabase:           defaultPostgresDatabase,
	}

	return pg, nil
}

// Close closes all connections to the postgres instance
func (pg *PostgresContainer) Close() {
	for _, pool := range pg.superUserConnections {
		pool.Close()
	}
}

// superUserConnection returns a connection to the postgres instance as the super user.
func (pg *PostgresContainer) superUserConnection(ctx context.Context, database string) (*pgxpool.Pool, error) {
	pg.sMu.Lock()
	defer pg.sMu.Unlock()

	pool, ok := pg.superUserConnections[database]
	if !ok || pool == nil || pool.Ping(ctx) != nil {
		var err error
		pool, err = openDB(ctx, pg.connectionURI(pg.superUserUsername, pg.password, database))
		if err != nil {
			return nil, err
		}
		pg.superUserConnections[database] = pool
	}

	return pool, nil
}

// CreateDatabase creates a new database with the given name and returns a connection to it.
func (pg *PostgresContainer) CreateDatabase(ctx context.Context, dbName string) (*PostgresDB, error) {
	dbName = pg.validDatabaseName(dbName)
	db, err := pg.superUserConnection(ctx, pg.defaultDatabase)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(ctx, fmt.Sprintf(`
		CREATE DATABASE %q WITH
			OWNER = %q
			ENCODING = 'UTF8'
			LC_COLLATE = 'en_US.utf8'
			LC_CTYPE = 'en_US.utf8'
			TABLESPACE = pg_default
			CONNECTION LIMIT = -1;
	`, dbName, pg.unpriviledgedUserUsername))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create database=%q", dbName)
	}

	// create extension in the newly created table
	db, err = openDB(ctx, pg.connectionURI(pg.superUserUsername, pg.password, dbName))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	_, err = db.Exec(ctx, `
		CREATE EXTENSION IF NOT EXISTS btree_gist
			SCHEMA public
			VERSION "1.5";
	`)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create extension btree_gist in database=%q", dbName)
	}

	u, err := openDB(ctx, pg.connectionURI(pg.unpriviledgedUserUsername, pg.password, dbName))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to database=%q with %s", dbName, pg.unpriviledgedUserUsername)
	}
	_, err = db.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA IF NOT EXISTS "%s";
	`, pg.unpriviledgedUserUsername))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create schema %q", pg.unpriviledgedUserUsername)
	}

	return &PostgresDB{
		Pool:   u,
		pg:     pg,
		dbName: dbName,
		schema: pg.unpriviledgedUserUsername,
	}, nil
}

func (pg *PostgresContainer) addUnprivilegedUser(ctx context.Context) error {
	db, err := pg.superUserConnection(ctx, pg.defaultDatabase)
	if err != nil {
		return err
	}

	if _, err := db.Exec(ctx, fmt.Sprintf(`
		CREATE USER %q WITH
			NOSUPERUSER
			NOCREATEDB
			NOCREATEROLE
			INHERIT
			NOREPLICATION
			CONNECTION LIMIT -1
			PASSWORD '%s';
	`, pg.unpriviledgedUserUsername, pg.password)); err != nil {
		return errors.Wrap(err, "failed to create unprivileged user")
	}

	return nil
}

func (pg *PostgresContainer) connectionURI(username, password, database string) string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=%s",
		username,
		password,
		pg.host,
		pg.port.Port(),
		database,
		pg.sslMode,
	)
}

// validDatabaseName returns a valid database name for postgres. It replaces all invalid characters with a valid one or removes them.
func (pg *PostgresContainer) validDatabaseName(dbName string) string {
	dbName = strings.ReplaceAll(dbName, "/", "_")
	dbName = strings.ReplaceAll(dbName, "#", "_")
	dbName = strings.ReplaceAll(dbName, "(", "")
	dbName = strings.ReplaceAll(dbName, ")", "")

	if l := len(dbName); l > 63 {
		pg.muReplacementCount.Lock()
		defer pg.muReplacementCount.Unlock()
		pg.replacementCount++
		uid := fmt.Sprintf("%d", pg.replacementCount)
		dbName = dbName[:29-len(uid)/2] + "-" + uid + "-" + dbName[l-30-len(uid)/2:]
	}

	return dbName
}

func openDB(ctx context.Context, connectionString string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, errors.Wrap(err, "pgxpool.ParseConfig()")
	}

	config.AfterConnect = func(_ context.Context, conn *pgx.Conn) error {
		shopspring.Register(conn.TypeMap())

		return nil
	}

	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.Wrapf(err, "pgxpool.NewWithConfig()")
	}

	return db, nil
}
