// Implements tooling to setup a Spanner database configured for running integration tests against.
package dbinitiator

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/docker/go-connections/nat"
	"github.com/go-playground/errors/v5"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultSpannerPort       = "9010/tcp"
	defaultSpannerProjectID  = "unit-testing"
	defaultSpannerInstanceID = "test-instance"
)

// SpannerContainer represents a docker container running a spanner instance.
type SpannerContainer struct {
	testcontainers.Container
	admin      *database.DatabaseAdminClient
	opts       []option.ClientOption
	port       string
	projectID  string
	instanceID string

	mu      sync.Mutex
	dbCount int
}

// NewSpannerContainer returns a initialized SpannerContainer ready to run to create databases for unit tests
func NewSpannerContainer(ctx context.Context, imageVersion string) (*SpannerContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "gcr.io/cloud-spanner-emulator/emulator:" + imageVersion,
		WaitingFor:   wait.ForLog("Cloud Spanner emulator running"),
		ExposedPorts: []string{defaultSpannerPort},
	}

	spannerC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started:          true,
		ContainerRequest: req,
	})
	if err != nil {
		return nil, errors.Wrap(err, "testcontainers.GenericContainer()")
	}

	host, err := spannerC.Host(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get host for container")
	}

	externalPort, err := spannerC.MappedPort(ctx, nat.Port(defaultSpannerPort))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get external port for exposed port %s", defaultSpannerPort)
	}

	endPoint := fmt.Sprintf("%s:%s", host, externalPort.Port())

	opts := []option.ClientOption{
		option.WithEndpoint(endPoint),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		internaloption.SkipDialSettingsValidation(),
	}

	if err := NewSpannerInstance(ctx, defaultSpannerProjectID, defaultSpannerInstanceID, opts...); err != nil {
		return nil, errors.Wrap(err, "failed to create spanner instance")
	}

	admin, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "database.NewDatabaseAdminClient()")
	}

	return &SpannerContainer{
		Container:  spannerC,
		admin:      admin,
		opts:       opts,
		port:       defaultSpannerPort,
		projectID:  defaultSpannerProjectID,
		instanceID: defaultSpannerInstanceID,
	}, nil
}

// CreateTestDatabase creates a database with dbName. Each test should create their own database for testing
func (sp *SpannerContainer) CreateTestDatabase(ctx context.Context, dbName string) (*SpannerDB, error) {
	dbName = sp.validDatabaseName(dbName)

	db, err := newSpannerDatabase(ctx, sp.admin, sp.projectID, sp.instanceID, dbName, sp.opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create spanner database %s", dbName)
	}

	return db, nil
}

// Close cleans up open resouces
func (sp *SpannerContainer) Close() error {
	if err := sp.admin.Close(); err != nil {
		return errors.Wrap(err, "database.DatabaseAdminClient.Close()")
	}

	return nil
}

func (sp *SpannerContainer) validDatabaseName(dbName string) string {
	b := []byte(dbName)
	b = bytes.ToLower(b)

	for i, v := range b {
		if !bytes.ContainsAny([]byte{v}, "1234567890abcdefghijklmnopqrstuvwxyz-_") {
			b[i] = '-'
		}
	}

	b = bytes.Trim(b, "-_")
	dbName = string(b)

	if l := len(dbName); l > 30 {
		sp.mu.Lock()
		defer sp.mu.Unlock()
		sp.dbCount++
		dbName = fmt.Sprintf("db%d-%s", sp.dbCount, dbName[l-20:])
	}

	return dbName
}
