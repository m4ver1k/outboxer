package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/italolelis/outboxer/lock"
)

const (
	// DefaultEventStoreTable is the default table name
	DefaultEventStoreTable = "event_store"
)

var (
	// ErrLocked is used when we can't acquire an explicit lock
	ErrLocked = errors.New("can't acquire lock")

	// ErrNoDatabaseName is used when the database name is blank
	ErrNoDatabaseName = errors.New("no database name")

	// ErrNoSchema is used when the schema name is blank
	ErrNoSchema = errors.New("no schema")
)

// SQLServer implementation of the data store
type SQLServer struct {
	conn     *sql.Conn
	isLocked bool

	SchemaName      string
	DatabaseName    string
	EventStoreTable string
}

// WithInstance creates a SQLServer data store with an existing db connection
func WithInstance(ctx context.Context, db *sql.DB) (*SQLServer, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	s := SQLServer{conn: conn}

	if err := conn.QueryRowContext(ctx, `SELECT DB_NAME()`).Scan(&s.DatabaseName); err != nil {
		return nil, err
	}

	if len(s.DatabaseName) == 0 {
		return nil, ErrNoDatabaseName
	}

	if err := conn.QueryRowContext(ctx, `SELECT SCHEMA_NAME()`).Scan(&s.SchemaName); err != nil {
		return nil, err
	}

	if len(s.SchemaName) == 0 {
		return nil, ErrNoSchema
	}

	if len(s.EventStoreTable) == 0 {
		s.EventStoreTable = DefaultEventStoreTable
	}

	if err := s.ensureTable(ctx); err != nil {
		return nil, err
	}

	return &s, nil
}

// Lock implements explicit locking
func (s *SQLServer) lock(ctx context.Context) error {
	if s.isLocked {
		return ErrLocked
	}

	aid, err := lock.Generate(s.DatabaseName, s.SchemaName)
	if err != nil {
		return err
	}

	query := `EXEC sp_getapplock 
	@Resource = @p1,
	@LockOwner='Session',
	@LockMode = 'Exclusive'; `

	if _, err := s.conn.ExecContext(ctx, query, aid); err != nil {
		return fmt.Errorf("try lock failed: %w", err)
	}

	s.isLocked = true
	return nil
}

// Unlock is the implementation of the unlock for explicit locking
func (s *SQLServer) unlock(ctx context.Context) error {
	if !s.isLocked {
		return nil
	}

	aid, err := lock.Generate(s.DatabaseName, s.SchemaName)
	if err != nil {
		return err
	}

	query := `EXEC sp_releaseapplock  
	@Resource = @p1, 
	@LockOwner='Session'; `

	if _, err := s.conn.ExecContext(ctx, query, aid); err != nil {
		return err
	}
	s.isLocked = false
	return nil
}
func (s *SQLServer) ensureTable(ctx context.Context) (err error) {
	if err = s.lock(ctx); err != nil {
		return err
	}

	defer func() {
		if e := s.unlock(ctx); e != nil {
			if err == nil {
				err = e
			} else {
				err = fmt.Errorf("failed to unlock table: %w", err)
			}
		}
	}()

	query := fmt.Sprintf(`
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='%[2]s' and xtype='U')
CREATE TABLE %[1]s.%[2]s (
	id int IDENTITY(1,1) NOT NULL PRIMARY KEY, 
	dispatched BIT NOT NULL default 0, 
	dispatched_at TIMESTAMP,
	payload VARBINARY(max)  not null,
	options NVARCHAR(MAX),
	headers NVARCHAR(MAX)
);
`, s.SchemaName, s.EventStoreTable)
	//TODO:
	// CREATE INDEX IF NOT EXISTS "index_dispatchedAt" ON %[1]s using btree (dispatched_at asc nulls last);
	// CREATE INDEX IF NOT EXISTS "index_dispatched" ON %[1]s using btree (dispatched asc nulls last);

	if _, err = s.conn.ExecContext(ctx, query); err != nil {
		return err
	}

	return nil
}

// Close closes the db connection
func (s *SQLServer) Close() error {
	if err := s.conn.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}
	return nil
}
