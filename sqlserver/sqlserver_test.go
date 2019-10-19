package sqlserver

import (
	"context"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/italolelis/outboxer/lock"
)

func TestSQLServer_WithInstance_must_return_SQLServerDataStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	initDatastoreMock(t, mock)
	ds, err := WithInstance(ctx, db)
	if err != nil {
		t.Fatalf("failed to setup the data store: %s", err)
	}
	defer ds.Close()

	if ds.SchemaName != "test_schema" {
		t.Errorf("Expected schema name %s but got %s", "test_schema", ds.SchemaName)
	}

	if ds.DatabaseName != "test" {
		t.Errorf("Expected database name %s but got %s", "test", ds.DatabaseName)
	}
}

func initDatastoreMock(t *testing.T, mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT DB_NAME() `).
		WillReturnRows(sqlmock.NewRows([]string{"DB_NAME()"}).AddRow("test"))
	mock.ExpectQuery(`SELECT SCHEMA_NAME()`).
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME()"}).AddRow("test_schema"))

	initLockMock(t, mock)
}

func initLockMock(t *testing.T, mock sqlmock.Sqlmock) {
	aid, err := lock.Generate("test", "test_schema")
	if err != nil {
		t.Fatalf("failed to generate the lock value: %s", err)
	}

	mock.ExpectExec(`EXEC sp_getapplock
	@Resource = @p1,
	@LockOwner='Session',
	@LockMode = 'Exclusive'; `).
		WithArgs(aid).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec(
		regexp.QuoteMeta(`IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='event_store' and xtype='U')
		CREATE TABLE test_schema.event_store`)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec(`EXEC sp_releaseapplock
	@Resource = @p1,
	@LockOwner='Session'; `).
		WithArgs(aid).
		WillReturnResult(sqlmock.NewResult(0, 1))
}
