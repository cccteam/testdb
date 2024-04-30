// Implements tooling to setup a Spanner database configured for running integration tests against.
package dbinitiator

import (
	"context"
	"testing"

	"github.com/docker/go-connections/nat"
	_ "github.com/golang-migrate/migrate/v4/database/spanner"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func TestSpanner_FullMigration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewSpannerContainer(ctx, "latest")
	if err != nil {
		t.Fatalf("New(): %s", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	type args struct {
		upSourceURL   []string
		downSourceURL string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		wantUpErr   bool
		wantDownErr bool
	}{
		{
			name: "FullMigration",
			args: args{
				upSourceURL:   []string{"file://testdata/spanner/migrations", "file://testdata/spanner/migrations2"},
				downSourceURL: "file://testdata/spanner/migrations",
			},
		},
		{
			name: "Migration up error",
			args: args{
				upSourceURL: []string{"file://testdata/spanner/migration_error"},
			},
			wantUpErr: true,
		},
		{
			name: "Migration up source error",
			args: args{
				upSourceURL: []string{"file://testdata/spanner/migration_does_not_exist"},
			},
			wantUpErr: true,
		},
		{
			name: "Migration down source error",
			args: args{
				upSourceURL:   []string{"file://testdata/spanner/migrations"},
				downSourceURL: "file://testdata/spanner/migration_does_not_exist",
			},
			wantDownErr: true,
		},
		{
			name: "Migration down error",
			args: args{
				upSourceURL:   []string{"file://testdata/spanner/migrations"},
				downSourceURL: "file://testdata/spanner/migration_error",
			},
			wantDownErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db, err := container.CreateTestDatabase(ctx, tt.name)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SpannerContainer.CreateTestDatabase() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			defer func() {
				if err := db.DropDatabase(context.Background()); err != nil {
					t.Fatalf("DB.DropDatabase() err=%s", err)
				}
				if err := db.Close(); err != nil {
					t.Fatalf("DB.Close() err=%s", err)
				}
			}()

			if err := db.MigrateUp(tt.args.upSourceURL...); (err != nil) != tt.wantUpErr {
				t.Fatalf("DB.MigrateUp() error = %v, wantUpErr %v", err, tt.wantUpErr)
			}
			if tt.wantUpErr {
				return
			}

			if err := db.MigrateDown(tt.args.downSourceURL); (err != nil) != tt.wantDownErr {
				t.Fatalf("db.MigrateDown() error = %v, wantDownErr %v", err, tt.wantDownErr)
			}
		})
	}
}

func TestNewSpannerContainer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name     string
		args     args
		wantHost string
		wantErr  bool
	}{
		{
			name: "Container with default host and port",
			args: args{
				ctx: context.Background(),
			},
			wantHost: "localhost",
		},
		{
			name: "Container error from canceled context",
			args: args{
				ctx: ctx,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			container, err := NewSpannerContainer(tt.args.ctx, "latest")
			if (err != nil) != tt.wantErr {
				t.Fatalf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			ports, err := container.Ports(tt.args.ctx)
			if err != nil {
				t.Fatalf("container.Ports() error = %v, wantErr %v", err, false)
			}
			if port, ok := ports[nat.Port(container.port)]; !ok {
				t.Fatalf("container.Ports() does not contain %s", container.port)
			} else if len(port) == 0 || port[0].HostPort == "" {
				t.Fatalf("container.Ports() does not export 9010/tcp")
			}
			host, err := container.Host(tt.args.ctx)
			if err != nil {
				t.Fatalf("container.Host() error = %v, wantErr %v", err, false)
			}
			if host != tt.wantHost {
				t.Fatalf("container.Host() = %v, wantHost %v", host, tt.wantHost)
			}
			state, err := container.State(tt.args.ctx)
			if err != nil {
				t.Fatalf("container.State() error = %v, wantErr %v", err, false)
			}
			if !state.Running {
				t.Fatalf("container.State() = %v, wantHost %v", state.Status, "running")
			}
		})
	}
}

func TestContainer_validDatabaseName(t *testing.T) {
	t.Parallel()

	type args struct {
		dbName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "valid name",
			args: args{dbName: "somedbname"},
			want: "somedbname",
		},
		{
			name: "Uppercase name to lower case",
			args: args{dbName: "SomeDBname"},
			want: "somedbname",
		},
		{
			name: "to long is truncated and prefixed",
			args: args{dbName: "0123456789012345678901234567890"},
			want: "db1-12345678901234567890",
		},
		{
			name: "invalid characters are replaced",
			args: args{dbName: "Some*DB.name"},
			want: "some-db-name",
		},
		{
			name: "underscore and hyphen are okay in middle",
			args: args{dbName: "Some_DB-name"},
			want: "some_db-name",
		},
		{
			name: "invalid leading or trailing characters are removed",
			args: args{dbName: "_SomeDBname-"},
			want: "somedbname",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sp := &SpannerContainer{}
			if got := sp.validDatabaseName(tt.args.dbName); got != tt.want {
				t.Errorf("Container.validDatabaseName() = %v, want %v", got, tt.want)
			}
		})
	}
}
