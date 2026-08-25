package db

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestGetInterval covers every duration bucket on both dialects. GetInterval
// is a pure function (no I/O) shared by both backends - each backend calls
// it from several sites - so one table here exercises everything either
// provider does with it; there is nothing
// provider-specific to test separately.
func TestGetInterval(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		duration     time.Duration
		wantPostgres string
		wantSQLite   string
	}{
		{name: "at the 2h boundary", duration: 2 * time.Hour, wantPostgres: "1 minute", wantSQLite: "+1 minutes"},
		{name: "just over 2h", duration: 2*time.Hour + time.Second, wantPostgres: "5 minutes", wantSQLite: "+5 minutes"},
		{name: "at the 6h boundary", duration: 6 * time.Hour, wantPostgres: "5 minutes", wantSQLite: "+5 minutes"},
		{name: "just over 6h", duration: 6*time.Hour + time.Second, wantPostgres: "15 minutes", wantSQLite: "+15 minutes"},
		{name: "at the 24h boundary", duration: 24 * time.Hour, wantPostgres: "15 minutes", wantSQLite: "+15 minutes"},
		{name: "just over 24h", duration: 24*time.Hour + time.Second, wantPostgres: "1 hour", wantSQLite: "+1 hours"},
		{name: "at the 7d boundary", duration: 7 * 24 * time.Hour, wantPostgres: "1 hour", wantSQLite: "+1 hours"},
		{name: "just over 7d", duration: 7*24*time.Hour + time.Second, wantPostgres: "6 hours", wantSQLite: "+6 hours"},
		{name: "at the 30d boundary", duration: 30 * 24 * time.Hour, wantPostgres: "6 hours", wantSQLite: "+6 hours"},
		{name: "just over 30d", duration: 30*24*time.Hour + time.Second, wantPostgres: "1 day", wantSQLite: "+1 days"},
		{name: "at the 90d boundary", duration: 90 * 24 * time.Hour, wantPostgres: "1 day", wantSQLite: "+1 days"},
		{name: "just over 90d (default branch)", duration: 90*24*time.Hour + time.Second, wantPostgres: "1 day", wantSQLite: "+1 days"},
		{name: "far beyond 90d (default branch)", duration: 365 * 24 * time.Hour, wantPostgres: "1 day", wantSQLite: "+1 days"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			to := base.Add(tt.duration)
			assert.Equal(t, tt.wantPostgres, GetInterval(base, to, "postgresql"), "postgresql")
			assert.Equal(t, tt.wantSQLite, GetInterval(base, to, "sqlite"), "sqlite")
		})
	}

	t.Run("unknown dialect falls back to the sqlite-style translation", func(t *testing.T) {
		// GetInterval only special-cases "postgresql"; any other dialect
		// string - including a typo or an unsupported dialect - intentionally
		// falls back to the SQLite "+N units" form.
		got := GetInterval(base, base.Add(time.Hour), "mysql")
		assert.Equal(t, "+1 minutes", got)
	})
}

// TestExecuteQuery verifies ExecuteQuery returns the query's rows on success
// and wraps the driver's error via QueryError, rather than passing it
// through raw, when the query itself fails.
func TestExecuteQuery(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	t.Run("success returns rows", func(t *testing.T) {
		rows, err := ExecuteQuery(context.Background(), db, "SELECT 1")
		assert.NoError(t, err)
		assert.NoError(t, rows.Close())
	})

	t.Run("query failure is wrapped", func(t *testing.T) {
		closed, err := sql.Open("sqlite", ":memory:")
		assert.NoError(t, err)
		assert.NoError(t, closed.Close())

		_, err = ExecuteQuery(context.Background(), closed, "SELECT 1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "executing query")
	})
}

// TestScanSingleRow verifies ScanSingleRow returns ErrNoResults on an empty
// result set, and wraps a scan failure via ErrorWithOperation rather than
// passing it through raw.
func TestScanSingleRow(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	t.Run("no rows returns ErrNoResults", func(t *testing.T) {
		rows, err := db.Query("SELECT 1 WHERE 1 = 0")
		assert.NoError(t, err)

		var dest int
		err = ScanSingleRow(rows, &dest)
		assert.ErrorIs(t, err, ErrNoResults)
	})

	t.Run("scan failure is wrapped", func(t *testing.T) {
		rows, err := db.Query("SELECT 1, 2")
		assert.NoError(t, err)

		var dest int
		err = ScanSingleRow(rows, &dest) // one destination, two columns
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "scanning row")
	})
}
