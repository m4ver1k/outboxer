package sqlserver_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/italolelis/outboxer/sqlserver"
)

func TestExampleSqlServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("sqlserver", "sqlserver://sa:C0mplexPwd@localhost:1433?database=outboxer&connection+timeout=30")

	if err != nil {
		fmt.Println("Failed to connect to SQLSERVER")
	}

	_, err = sqlserver.WithInstance(ctx, db)
	if err != nil {
		fmt.Println("Something wring ", err)
	}
}
