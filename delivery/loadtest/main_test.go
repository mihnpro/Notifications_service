package loadtest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// dbPool is shared across all tests in this package.
// It is nil when the database is unavailable — tests check this and skip.
var dbPool *pgxpool.Pool

func TestMain(m *testing.M) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = "postgres://notifications:notifications@localhost:5433/notifications"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadtest: cannot create pool, skipping all tests:", err)
		os.Exit(0)
	}
	if err := pool.Ping(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest: DB ping failed, skipping all tests:", err)
		os.Exit(0)
	}

	dbPool = pool
	defer pool.Close()
	os.Exit(m.Run())
}
