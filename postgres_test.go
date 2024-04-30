package dbinitializer

import (
	"context"
	"testing"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func TestPostgres_FullMigration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	container, err := NewPostgresContainer(ctx)
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
				upSourceURL:   []string{"file://testdata/postgres/migrations"},
				downSourceURL: "file://testdata/postgres/migrations",
			},
		},
		{
			name: "Migration up error",
			args: args{
				upSourceURL: []string{"file://testdata/postgres/migration_error"},
			},
			wantUpErr: true,
		},
		{
			name: "Migration down error",
			args: args{
				upSourceURL:   []string{"file://testdata/postgres/migrations"},
				downSourceURL: "file://testdata/postgres/migration_error",
			},
			wantDownErr: true,
		},
		{
			name: "Migration up source error",
			args: args{
				upSourceURL: []string{"file://testdata/postgres/migration_does_not_exist"},
			},
			wantUpErr: true,
		},
		{
			name: "Migration down source error",
			args: args{
				upSourceURL:   []string{"file://testdata/postgres/migrations"},
				downSourceURL: "file://testdata/postgres/migration_does_not_exist",
			},
			wantDownErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db, err := container.CreateDatabase(ctx, tt.name)
			if (err != nil) != tt.wantErr {
				t.Fatalf("PostgresContainer.CreateDatabase() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			if err := db.MigrateUp(tt.args.upSourceURL...); (err != nil) != tt.wantUpErr {
				t.Fatalf("db.MigrateUp() error = %v, wantUpErr %v", err, tt.wantUpErr)
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
