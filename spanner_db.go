package dbinitiator

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	spannerDB "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instanceadm "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/go-playground/errors/v5"
	"github.com/golang-migrate/migrate/v4"
	migratedb "github.com/golang-migrate/migrate/v4/database"
	spannerDriver "github.com/golang-migrate/migrate/v4/database/spanner"
	"google.golang.org/api/option"
)

// SpannerDB represents a database created and ready for migrations
type SpannerDB struct {
	dbStr      string
	admin      *spannerDB.DatabaseAdminClient
	closeAdmin bool
	*spanner.Client
}

// NewSpannerDatabase will create a spanner database
func NewSpannerDatabase(ctx context.Context, projectID, instanceID, dbName string, opts ...option.ClientOption) (*SpannerDB, error) {
	adminClient, err := spannerDB.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "database.NewDatabaseAdminClient()")
	}

	db, err := newSpannerDatabase(ctx, adminClient, projectID, instanceID, dbName, opts...)
	if err != nil {
		adminClient.Close()

		return nil, err
	}

	db.closeAdmin = true

	return db, nil
}

func newSpannerDatabase(ctx context.Context, adminClient *spannerDB.DatabaseAdminClient, projectID, instanceID, dbName string, opts ...option.ClientOption) (*SpannerDB, error) {
	dbStr := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	client, err := spanner.NewClient(ctx, dbStr, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "spanner.NewClient()")
	}

	op, err := adminClient.CreateDatabase(ctx,
		&databasepb.CreateDatabaseRequest{
			Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
			CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", dbName),
		},
	)
	if err != nil {
		return nil, errors.Wrapf(err, "database.DatabaseAdminClient.CreateDatabase()")
	}

	if _, err := op.Wait(ctx); err != nil {
		return nil, errors.Wrapf(err, "database.CreateDatabaseOperation.Wait()")
	}

	return &SpannerDB{
		dbStr:  dbStr,
		admin:  adminClient,
		Client: client,
	}, nil
}

// MigrateUp will migrate all the way up, applying all up migrations from all sourceURL's
func (db *SpannerDB) MigrateUp(sourceURL ...string) error {
	conf := &spannerDriver.Config{DatabaseName: db.dbStr, CleanStatements: true, DoNotCloseSpannerClients: true}
	spannerInstance, err := spannerDriver.WithInstance(spannerDriver.NewDB(*db.admin, *db.Client), conf)
	if err != nil {
		return errors.Wrap(err, "spannerDriver.WithInstance()")
	}

	for _, source := range sourceURL {
		if err := db.migrateUp(source, spannerInstance); err != nil {
			return err
		}
	}

	return nil
}

func (db *SpannerDB) migrateUp(source string, spannerInstance migratedb.Driver) error {
	m, err := migrate.NewWithDatabaseInstance(source, "spanner", spannerInstance)
	if err != nil {
		return errors.Wrapf(err, "migrate.NewWithDatabaseInstance(): fileURL=%s, db=%s", source, db.dbStr)
	}
	defer m.Close()

	if _, _, err := m.Version(); !errors.Is(err, migrate.ErrNilVersion) {
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

	return nil
}

// MigrateDown will migrate all the way down
func (db *SpannerDB) MigrateDown(sourceURL string) error {
	conf := &spannerDriver.Config{DatabaseName: db.dbStr, CleanStatements: true, DoNotCloseSpannerClients: true}
	spannerInstance, err := spannerDriver.WithInstance(spannerDriver.NewDB(*db.admin, *db.Client), conf)
	if err != nil {
		return errors.Wrap(err, "spannerDriver.WithInstance()")
	}

	m, err := migrate.NewWithDatabaseInstance(sourceURL, "spanner", spannerInstance)
	if err != nil {
		return errors.Wrapf(err, "migrate.NewWithDatabaseInstance(): fileURL=%s, db=%s", sourceURL, db.dbStr)
	}
	defer m.Close()

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

func (db *SpannerDB) DropDatabase(ctx context.Context) error {
	if err := db.admin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: db.dbStr}); err != nil {
		return errors.Wrap(err, "database.DatabaseAdminClient.DropDatabase()")
	}

	return nil
}

func (db *SpannerDB) Close() error {
	db.Client.Close()

	if db.closeAdmin {
		if err := db.admin.Close(); err != nil {
			return errors.Wrap(err, "database.DatabaseAdminClient.Close()")
		}
	}

	return nil
}

// NewSpannerInstance creates a spanner instance. This is intended for use with a spanner emulator.
func NewSpannerInstance(ctx context.Context, projectID, instanceID string, opts ...option.ClientOption) error {
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		return errors.Wrap(err, "instanceadmin.NewInstanceAdminClient()")
	}
	defer instanceAdmin.Close()

	op, err := instanceAdmin.CreateInstance(ctx,
		&instanceadm.CreateInstanceRequest{
			Parent:     fmt.Sprintf("projects/%s", projectID),
			InstanceId: instanceID,
			Instance: &instanceadm.Instance{
				DisplayName: instanceID,
			},
		},
	)
	if err != nil {
		return errors.Wrapf(err, "instanceadmin.InstanceAdminClient.CreateInstance()")
	}

	i, err := op.Wait(ctx)
	if err != nil {
		return errors.Wrapf(err, "instanceadmin.CreateInstanceOperation.Wait()")
	}
	if i.State != instanceadm.Instance_READY {
		return errors.Newf("instanceadmin.CreateInstanceOperation.Wait(): State = %v", i.State)
	}

	return nil
}
