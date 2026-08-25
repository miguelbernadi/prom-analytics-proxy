package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"
)

// ExecuteQuery is a helper function to execute a query with error handling
func ExecuteQuery(ctx context.Context, db *sql.DB, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, QueryError(err, "executing query", query)
	}
	return rows, nil
}

// CloseResource safely closes a resource and logs any errors
func CloseResource(closer io.Closer) {
	if closer == nil {
		return
	}
	if err := closer.Close(); err != nil {
		slog.Error("Error closing resource", "error", err)
	}
}

// ScanSingleRow scans a single row with proper error handling
func ScanSingleRow(rows *sql.Rows, dest ...interface{}) error {
	defer CloseResource(rows)

	if !rows.Next() {
		return ErrNoResults
	}

	if err := rows.Scan(dest...); err != nil {
		return ErrorWithOperation(err, "scanning row")
	}

	if err := rows.Err(); err != nil {
		return ErrorWithOperation(err, "row iteration")
	}

	return nil
}

// countMatching runs a standalone COUNT query and returns the scalar result.
// A paginated method must run this independently of its paged query - a
// count read off a paged row is wrong once LIMIT/OFFSET selects zero rows
// (a page past the last one), since there is then no row left to read it
// from.
func countMatching(ctx context.Context, db *sql.DB, countQuery string, args ...interface{}) (int, error) {
	var n int
	if err := db.QueryRowContext(ctx, countQuery, args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// PrepareTimeRange formats time range parameters based on the database dialect
func PrepareTimeRange(tr TimeRange, dialect string) (string, string) {
	if dialect == "postgresql" {
		return tr.Format(ISOTimeFormat)
	}
	return tr.Format(SQLiteTimeFormat)
}

// warnIfSummaryRefreshWasNoOp logs when a summary refresh's INSERT touched
// zero rows while metrics_catalog is non-empty. That combination means every
// catalog row failed the last_synced_at freshness filter - most likely
// metadata sync has stopped advancing last_synced_at (disabled, or a
// seen_ttl raised above inventory.time_window) - which otherwise surfaces as
// a silently frozen summary with no error. See
// https://github.com/nicolastakashi/prom-analytics-proxy/issues/579.
func warnIfSummaryRefreshWasNoOp(ctx context.Context, db *sql.DB, rowsAffected int64, dialect string) {
	if rowsAffected > 0 {
		return
	}
	var catalogNonEmpty bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM metrics_catalog)`).Scan(&catalogNonEmpty); err != nil {
		return // best-effort diagnostic; don't fail the refresh over it
	}
	if catalogNonEmpty {
		slog.Warn("inventory: refresh summary touched no rows despite a non-empty catalog; "+
			"every row failed the last_synced_at freshness filter - check that metadata sync is advancing last_synced_at",
			"dialect", dialect)
	}
}

// execRefreshSummary runs RefreshMetricsUsageSummary's INSERT ... ON
// CONFLICT statement and surfaces the zero-rows-affected warning
// (warnIfSummaryRefreshWasNoOp) - the only two things both backends do
// identically around their own dialect-specific query text and bind args.
func execRefreshSummary(ctx context.Context, db *sql.DB, query string, dialect string, args ...any) error {
	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("refresh summary: %w", err)
	}
	if n, err := res.RowsAffected(); err == nil {
		warnIfSummaryRefreshWasNoOp(ctx, db, n, dialect)
	}
	return nil
}

// listJobs returns the distinct list of jobs known in metrics_job_index.
// Shared verbatim across backends - metrics_job_index has no dialect-specific
// columns or types, so there is nothing for either provider to specialize.
func listJobs(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := ExecuteQuery(ctx, db, `SELECT DISTINCT job FROM metrics_job_index ORDER BY job`)
	if err != nil {
		return nil, err
	}
	defer CloseResource(rows)

	var jobs []string
	for rows.Next() {
		var job string
		if err := rows.Scan(&job); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter jobs: %w", err)
	}
	return jobs, nil
}

// execUpsertMany runs one INSERT/upsert statement per item inside a single
// transaction, sharing the prepare/loop/commit/rollback control flow that
// would otherwise be duplicated identically across backends. query must be
// the complete, dialect-correct SQL text - including that backend's own
// placeholder style and any conflict-clause casing - execUpsertMany does not
// touch the SQL itself, only the Go control flow around running it once per
// item. Callers remain responsible for their own locking (e.g. SQLite's
// single-writer mutex) and any pre-processing (e.g. deduplication).
//
// items must already be in the order the caller wants upserted: this
// function does not sort. Callers must pre-sort items by their ON CONFLICT
// target, so concurrent calls with overlapping rows always acquire row locks
// in the same order regardless of caller-supplied order - avoiding
// Postgres's own documented deadlock precondition for concurrent upserts
// against overlapping rows.
func execUpsertMany[T any](ctx context.Context, db *sql.DB, query string, items []T, argsFn func(T) []any) error {
	if len(items) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare: %w", err)
	}
	defer CloseResource(stmt)
	for _, it := range items {
		if _, err := stmt.ExecContext(ctx, argsFn(it)...); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("exec: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// normalizeRulesUsage de-duplicates by (Serie, Kind, GroupName, Name,
// Expression, sorted Labels) - the ON CONFLICT target both backends upsert
// RulesUsage against - and sorts the result by the same composite key. Both
// backends need this identical treatment before upserting: the sort is
// required independently of dedup, so concurrent calls with overlapping
// rows always acquire row locks in the same order regardless of
// caller-supplied order, avoiding Postgres's own documented deadlock
// precondition for concurrent upserts against overlapping rows. Every field
// in the key is part of RulesUsage's identity, so first-vs-last occurrence
// within one call is moot for de-duplication - two "duplicate" rows are
// identical in every field that matters.
func normalizeRulesUsage(items []RulesUsage) ([]RulesUsage, error) {
	type ruleKey struct {
		Serie, Kind, Group, Name, Expression, Labels string
	}
	dedup := make(map[ruleKey]struct{}, len(items))
	normalized := make([]RulesUsage, 0, len(items))
	for _, r := range items {
		// Normalize labels order for stable JSON equality.
		labels := make([]string, len(r.Labels))
		copy(labels, r.Labels)
		sort.Strings(labels)
		labelsJSON, err := json.Marshal(labels)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal labels to JSON: %w", err)
		}
		k := ruleKey{Serie: r.Serie, Kind: r.Kind, Group: r.GroupName, Name: r.Name, Expression: r.Expression, Labels: string(labelsJSON)}
		if _, ok := dedup[k]; ok {
			continue
		}
		dedup[k] = struct{}{}
		r.Labels = labels
		normalized = append(normalized, r)
	}
	sort.Slice(normalized, func(i, j int) bool {
		a, b := normalized[i], normalized[j]
		if a.Serie != b.Serie {
			return a.Serie < b.Serie
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.GroupName != b.GroupName {
			return a.GroupName < b.GroupName
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if a.Expression != b.Expression {
			return a.Expression < b.Expression
		}
		// labels is part of the ON CONFLICT target too - two rows can
		// share every other field and still be distinct conflict targets
		// differing only in labels. Each item's Labels is already sorted
		// (above), so joining is a stable, comparable form.
		return strings.Join(a.Labels, ",") < strings.Join(b.Labels, ",")
	})
	return normalized, nil
}

// dedupDashboardUsage de-duplicates by (Id, Serie) pair, keeping the last
// entry, and sorts the result by the same pair - the ON CONFLICT target both
// backends upsert DashboardUsage against. Both backends need this identical
// treatment before upserting: the sort is required independently of dedup,
// so concurrent calls with overlapping rows always acquire row locks in the
// same order regardless of caller-supplied order, avoiding Postgres's own
// documented deadlock precondition for concurrent upserts against
// overlapping rows.
func dedupDashboardUsage(items []DashboardUsage) []DashboardUsage {
	type dashKey struct{ Id, Serie string }
	dedup := make(map[dashKey]DashboardUsage, len(items))
	for _, d := range items {
		dedup[dashKey{Id: d.Id, Serie: d.Serie}] = d
	}
	out := make([]DashboardUsage, 0, len(dedup))
	for _, d := range dedup {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Id != out[j].Id {
			return out[i].Id < out[j].Id
		}
		return out[i].Serie < out[j].Serie
	})
	return out
}

// GetInterval returns the appropriate interval string for time-based queries
func GetInterval(from, to time.Time, dialect string) string {
	duration := to.Sub(from)
	var interval string

	switch {
	case duration <= 2*time.Hour:
		interval = "1 minute"
	case duration <= 6*time.Hour:
		interval = "5 minutes"
	case duration <= 24*time.Hour:
		interval = "15 minutes"
	case duration <= 7*24*time.Hour:
		interval = "1 hour"
	case duration <= 30*24*time.Hour:
		interval = "6 hours"
	case duration <= 90*24*time.Hour:
		interval = "1 day"
	default:
		interval = "1 day"
	}

	if dialect == "postgresql" {
		return interval
	}

	// SQLite has a different interval syntax
	switch interval {
	case "1 minute":
		return "+1 minutes"
	case "5 minutes":
		return "+5 minutes"
	case "15 minutes":
		return "+15 minutes"
	case "1 hour":
		return "+1 hours"
	case "6 hours":
		return "+6 hours"
	case "1 day":
		return "+1 days"
	default:
		return "+1 days"
	}
}
