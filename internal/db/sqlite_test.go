package db

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestSQLiteProvider returns a fresh provider and a cleanup function.
// newSqliteProvider opens SQLite with an empty DSN, which per SQLite's own
// semantics gives each connection a private, temporary on-disk database that
// is deleted automatically when the connection closes - no test-owned file
// to create or remove.
func newTestSQLiteProvider(t *testing.T) (Provider, func()) {
	t.Helper()
	provider, err := newSqliteProvider(context.Background())
	require.NoError(t, err, "failed to init sqlite provider")

	return provider, func() { _ = provider.Close() }
}

// -------------------- Analytics --------------------

func TestSQLite_GetQueryTypes(t *testing.T) { testGetQueryTypes(t, newTestSQLiteProvider) }

func TestSQLite_GetAverageDuration(t *testing.T) { testGetAverageDuration(t, newTestSQLiteProvider) }

func TestSQLite_GetQueryRate(t *testing.T) { testGetQueryRate(t, newTestSQLiteProvider) }

func TestSQLite_GetQueryLatencyTrends_And_Throughput_And_Errors(t *testing.T) {
	testGetQueryLatencyTrendsAndThroughputAndErrors(t, newTestSQLiteProvider)
}

func TestSQLite_AnalyticsMethodsOnEmptyDatabase(t *testing.T) {
	testAnalyticsMethodsOnEmptyDatabase(t, newTestSQLiteProvider)
}

// -------------------- Aggregations --------------------

func TestSQLite_GetQueriesBySerieName(t *testing.T) {
	testGetQueriesBySerieName(t, newTestSQLiteProvider)
}

func TestSQLite_GetQueriesBySerieName_Pagination(t *testing.T) {
	testGetQueriesBySerieNamePagination(t, newTestSQLiteProvider)
}

func TestSQLite_GetQueryExpressions_And_Executions(t *testing.T) {
	testGetQueryExpressionsAndExecutions(t, newTestSQLiteProvider)
}

func TestSQLite_GetQueryExpressions_Pagination(t *testing.T) {
	testGetQueryExpressionsPagination(t, newTestSQLiteProvider)
}

func TestSQLite_GetQueryExecutions_Pagination_TypeFilter_And_HTTPHeaders(t *testing.T) {
	testGetQueryExecutionsPaginationTypeFilterAndHTTPHeaders(t, newTestSQLiteProvider)
}

// -------------------- Metrics Inventory --------------------

func TestSQLite_MetricsJobIndex_And_ListJobs(t *testing.T) {
	testMetricsJobIndexAndListJobs(t, newTestSQLiteProvider)
}

func TestSQLite_RefreshMetricsUsageSummary_And_GetSeriesMetadata(t *testing.T) {
	testRefreshMetricsUsageSummaryAndGetSeriesMetadata(t, newTestSQLiteProvider)
}

// mustSummaryRowSQLite reads back one metrics_usage_summary row's four usage
// counts plus is_unused. Callers assert on the whole row rather than a
// single count: is_unused is derived from all four, so seeing the whole row
// is what tells you which one is off if this ever fails again.
func mustSummaryRowSQLite(t *testing.T, p Provider, name string) summaryRow {
	t.Helper()
	var r summaryRow
	p.WithDB(func(d *sql.DB) {
		row := d.QueryRowContext(context.Background(),
			`SELECT alert_count, record_count, dashboard_count, query_count, is_unused FROM metrics_usage_summary WHERE name = ?`, name)
		require.NoError(t, row.Scan(&r.Alert, &r.Record, &r.Dashboard, &r.Query, &r.Unused))
	})
	return r
}

// TestSQLite_RefreshMetricsUsageSummary_ExcludesStaleCatalogRows is the
// regression test for
// https://github.com/nicolastakashi/prom-analytics-proxy/issues/579: a
// catalog row Prometheus stopped reporting long ago (last_synced_at far in
// the past) must not be recomputed on every refresh just because some usage
// evidence happens to fall inside the window - it should be skipped
// entirely, keeping whatever summary it last had (here, its placeholder
// zero-count row from UpsertMetricsCatalog, is_unused=false per
// https://github.com/nicolastakashi/prom-analytics-proxy/issues/570:
// "never evaluated" is distinct from "confirmed unused").
func TestSQLite_RefreshMetricsUsageSummary_ExcludesStaleCatalogRows(t *testing.T) {
	p, cleanup := newTestSQLiteProvider(t)
	defer cleanup()

	mustUpsertCatalog(t, p, []MetricCatalogItem{
		{Name: "fresh_metric", Type: "gauge", Help: "still scraped"},
		{Name: "stale_metric", Type: "gauge", Help: "no longer scraped"},
	})

	// Backdate stale_metric well outside any window the refresh below uses,
	// simulating a metric Prometheus stopped reporting long ago.
	p.WithDB(func(d *sql.DB) {
		_, err := d.ExecContext(context.Background(),
			`UPDATE metrics_catalog SET last_synced_at = datetime('now', '-90 days') WHERE name = ?`, "stale_metric")
		assert.NoError(t, err, "backdate stale_metric")
	})

	now := time.Now().UTC()
	// Usage evidence for BOTH metrics, inside the refresh window below - if
	// the catalog-freshness filter didn't exist, both would come out used.
	mustInsertQueries(t, p, []Query{
		{
			TS: now.Add(-time.Minute), QueryParam: "fresh_metric", TimeParam: now,
			Duration: time.Millisecond, StatusCode: 200,
			LabelMatchers: LabelMatchers{{"__name__": "fresh_metric"}}, Type: QueryTypeInstant,
		},
		{
			TS: now.Add(-time.Minute), QueryParam: "stale_metric", TimeParam: now,
			Duration: time.Millisecond, StatusCode: 200,
			LabelMatchers: LabelMatchers{{"__name__": "stale_metric"}}, Type: QueryTypeInstant,
		},
	})

	err := p.RefreshMetricsUsageSummary(context.Background(), TimeRange{From: now.Add(-time.Hour), To: now})
	assert.NoError(t, err, "RefreshMetricsUsageSummary")

	fresh := mustSummaryRowSQLite(t, p, "fresh_metric")
	assert.Greater(t, fresh.Query, 0, "fresh_metric should have been recomputed with real usage")
	assert.False(t, fresh.Unused, "fresh_metric should be marked used")

	stale := mustSummaryRowSQLite(t, p, "stale_metric")
	assert.Equal(t, summaryRow{Alert: 0, Record: 0, Dashboard: 0, Query: 0, Unused: false}, stale,
		"stale_metric's summary should remain untouched at its placeholder values - got %+v", stale)
}

// TestSQLite_RefreshMetricsUsageSummary_ExcludesOutOfWindowRulesUsage guards
// RulesUsage's own presence-window filter in RefreshMetricsUsageSummary: a rule
// whose first_seen_at/last_seen_at fall outside the refresh's TimeRange must not
// count toward alert_count/record_count. See
// https://github.com/nicolastakashi/prom-analytics-proxy/issues/589.
func TestSQLite_RefreshMetricsUsageSummary_ExcludesOutOfWindowRulesUsage(t *testing.T) {
	p, cleanup := newTestSQLiteProvider(t)
	defer cleanup()

	mustUpsertCatalog(t, p, []MetricCatalogItem{
		{Name: "in_window_metric", Type: "gauge", Help: "actively alerting"},
		{Name: "out_of_window_metric", Type: "gauge", Help: "alert retired long ago"},
	})
	mustInsertRules(t, p, []RulesUsage{
		{Serie: "in_window_metric", GroupName: "g", Name: "a1", Expression: "e", Kind: string(RuleUsageKindAlert), Labels: []string{"l"}},
		{Serie: "out_of_window_metric", GroupName: "g", Name: "a2", Expression: "e", Kind: string(RuleUsageKindAlert), Labels: []string{"l"}},
	})

	// InsertRulesUsage always stamps first_seen_at/last_seen_at at call time,
	// so there's no public way to seed a rule outside the presence window -
	// push it out directly, simulating a rule retired long ago.
	p.WithDB(func(d *sql.DB) {
		_, err := d.ExecContext(context.Background(),
			`UPDATE RulesUsage SET first_seen_at = datetime('now', '-100 days'), last_seen_at = datetime('now', '-99 days') WHERE serie = ?`,
			"out_of_window_metric")
		assert.NoError(t, err, "backdate out_of_window_metric's rule presence")
	})

	// To is a minute past "now": time-range formatting truncates to whole
	// seconds, and the rule just inserted carries sub-second precision - an
	// unpadded "now" bound can land before the rule's own timestamp and
	// spuriously exclude it.
	now := time.Now().UTC()
	err := p.RefreshMetricsUsageSummary(context.Background(),
		TimeRange{From: now.Add(-30 * 24 * time.Hour), To: now.Add(time.Minute)})
	assert.NoError(t, err, "RefreshMetricsUsageSummary")

	inWindow := mustSummaryRowSQLite(t, p, "in_window_metric")
	assert.Equal(t, summaryRow{Alert: 1, Record: 0, Dashboard: 0, Query: 0, Unused: false}, inWindow,
		"in_window_metric's alert rule falls inside the refresh window and must be counted - got %+v", inWindow)

	outOfWindow := mustSummaryRowSQLite(t, p, "out_of_window_metric")
	assert.Equal(t, summaryRow{Alert: 0, Record: 0, Dashboard: 0, Query: 0, Unused: true}, outOfWindow,
		"out_of_window_metric's rule presence ended before the refresh window started, so it must not be counted as used - got %+v", outOfWindow)
}

// TestSQLite_RefreshMetricsUsageSummary_WarnsWhenAllRowsAreStale guards a
// suggestion adjacent to
// https://github.com/nicolastakashi/prom-analytics-proxy/issues/579: when
// every metrics_catalog row fails the freshness filter, the refresh's
// INSERT touches zero rows and returns no error - indistinguishable,
// without a log line, from "ran fine, nothing needed recomputing".
// warnIfSummaryRefreshWasNoOp surfaces that combination so metadata-sync
// stalling doesn't freeze summaries silently.
func TestSQLite_RefreshMetricsUsageSummary_WarnsWhenAllRowsAreStale(t *testing.T) {
	p, cleanup := newTestSQLiteProvider(t)
	defer cleanup()

	mustUpsertCatalog(t, p, []MetricCatalogItem{{Name: "stale_metric", Type: "gauge", Help: "gone"}})

	p.WithDB(func(d *sql.DB) {
		_, err := d.ExecContext(context.Background(),
			`UPDATE metrics_catalog SET last_synced_at = datetime('now', '-90 days') WHERE name = ?`, "stale_metric")
		assert.NoError(t, err, "backdate stale_metric")
	})

	var logs bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(prevLogger)

	now := time.Now().UTC()
	err := p.RefreshMetricsUsageSummary(context.Background(), TimeRange{From: now.Add(-time.Hour), To: now})
	assert.NoError(t, err, "RefreshMetricsUsageSummary")

	assert.Contains(t, logs.String(), "refresh summary touched no rows",
		"expected a warning when the freshness filter matches zero rows against a non-empty catalog")
}

// TestSQLite_RefreshMetricsUsageSummary_NoWarnOnEmptyCatalog is the
// zero-rows counterpart of TestSQLite_RefreshMetricsUsageSummary_WarnsWhenAllRowsAreStale:
// an empty metrics_catalog also touches zero rows, but that's the ordinary
// steady state of a fresh deployment, not a stalled sync - it must not log
// the same warning.
func TestSQLite_RefreshMetricsUsageSummary_NoWarnOnEmptyCatalog(t *testing.T) {
	p, cleanup := newTestSQLiteProvider(t)
	defer cleanup()

	var logs bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(prevLogger)

	now := time.Now().UTC()
	err := p.RefreshMetricsUsageSummary(context.Background(), TimeRange{From: now.Add(-time.Hour), To: now})
	assert.NoError(t, err, "RefreshMetricsUsageSummary")

	assert.NotContains(t, logs.String(), "refresh summary touched no rows",
		"an empty catalog is the normal steady state and must not be logged as a stalled sync")
}

func TestSQLite_GetMetricStatistics_And_QueryPerformanceStats(t *testing.T) {
	testGetMetricStatisticsAndQueryPerformanceStats(t, newTestSQLiteProvider)
}

// -------------------- Rules & Dashboards --------------------

func TestSQLite_InsertRulesUsage_GetRulesUsage(t *testing.T) {
	testInsertRulesUsageGetRulesUsage(t, newTestSQLiteProvider)
}

func TestSQLite_GetRulesUsage_SortFieldsAndPagination(t *testing.T) {
	testGetRulesUsageSortFieldsAndPagination(t, newTestSQLiteProvider)
}

func TestSQLite_InsertDashboardUsage_UpsertBehavior(t *testing.T) {
	testInsertDashboardUsageUpsertBehavior(t, newTestSQLiteProvider)
}

func TestSQLite_GetDashboardUsage_SortFieldsAndPagination(t *testing.T) {
	testGetDashboardUsageSortFieldsAndPagination(t, newTestSQLiteProvider)
}

// TestSQLite_GetRulesUsage_MaliciousSortOrderDoesNotBreakQuery is
// TestPostgreSQL_GetRulesUsage_MaliciousSortOrderDoesNotBreakQuery's
// counterpart. SQLite parameterizes SortOrder rather than interpolating it,
// so it was never exploitable the same way, but it still goes through
// ValidateSortField for the same default/whitelist guarantee, including
// correct handling of uppercase input.
func TestSQLite_GetRulesUsage_MaliciousSortOrderDoesNotBreakQuery(t *testing.T) {
	p, cleanup := newTestSQLiteProvider(t)
	defer cleanup()

	now := time.Now().UTC()
	mustInsertRules(t, p, []RulesUsage{
		{Serie: "up", GroupName: "g1", Name: "r1", Expression: "e1", Kind: string(RuleUsageKindAlert), Labels: []string{"l"}},
		{Serie: "up", GroupName: "g2", Name: "r2", Expression: "e2", Kind: string(RuleUsageKindAlert), Labels: []string{"l"}},
	})

	out, err := p.GetRulesUsage(context.Background(), RulesUsageParams{
		Serie:     "up",
		Kind:      string(RuleUsageKindAlert),
		TimeRange: TimeRange{From: now.Add(-1 * time.Hour), To: now.Add(1 * time.Hour)},
		Page:      1,
		PageSize:  10,
		SortBy:    "name",
		SortOrder: "asc; DROP TABLE RulesUsage; --",
	})
	assert.NoError(t, err, "GetRulesUsage must not error on an invalid SortOrder")
	rows, ok := out.Data.([]RulesUsage)
	if assert.True(t, ok, "type conversion") {
		assert.Len(t, rows, 2,
			"both rules must still be returned - an invalid SortOrder must fall back to a safe default order, not corrupt the result set")
	}
}

// TestSQLite_GetDashboardUsage_MaliciousSortOrderDoesNotBreakQuery is
// TestPostgreSQL_GetDashboardUsage_MaliciousSortOrderDoesNotBreakQuery's
// counterpart; see TestSQLite_GetRulesUsage_MaliciousSortOrderDoesNotBreakQuery
// for why SQLite was never exploitable the same way.
func TestSQLite_GetDashboardUsage_MaliciousSortOrderDoesNotBreakQuery(t *testing.T) {
	p, cleanup := newTestSQLiteProvider(t)
	defer cleanup()

	base := time.Now().UTC().Truncate(time.Minute)
	mustInsertDashboards(t, p, []DashboardUsage{
		{Id: "d1", Serie: "m1", Name: "Dash 1", URL: "http://d/1", CreatedAt: base},
		{Id: "d2", Serie: "m1", Name: "Dash 2", URL: "http://d/2", CreatedAt: base},
	})

	out, err := p.GetDashboardUsage(context.Background(), DashboardUsageParams{
		Serie:     "m1",
		TimeRange: TimeRange{From: base.Add(-1 * time.Hour), To: base.Add(1 * time.Hour)},
		Page:      1,
		PageSize:  10,
		SortBy:    "name",
		SortOrder: "asc; DROP TABLE DashboardUsage; --",
	})
	assert.NoError(t, err, "GetDashboardUsage must not error on an invalid SortOrder")
	rows, ok := out.Data.([]DashboardUsage)
	if assert.True(t, ok, "type conversion") {
		assert.Len(t, rows, 2,
			"both dashboards must still be returned - an invalid SortOrder must fall back to a safe default order, not corrupt the result set")
	}
}

// -------------------- Metrics catalog / inventory / usage --------------------

func TestSQLite_HistogramSummaryMetricsCatalog(t *testing.T) {
	testHistogramSummaryMetricsCatalog(t, newTestSQLiteProvider)
}

func TestSQLite_MetricsInventoryAndList(t *testing.T) {
	testMetricsInventoryAndList(t, newTestSQLiteProvider)
}

func TestSQLite_GetSeriesMetadata_UsageFilters(t *testing.T) {
	testGetSeriesMetadataUsageFilters(t, newTestSQLiteProvider)
}

func TestSQLite_GetSeriesMetadata_EmptyResults(t *testing.T) {
	testGetSeriesMetadataEmptyResults(t, newTestSQLiteProvider)
}

func TestSQLite_GetSeriesMetadataUnusedJobScoped(t *testing.T) {
	testGetSeriesMetadataUnusedJobScoped(t, newTestSQLiteProvider)
}

// TestSQLite_UpsertMetricsCatalog_CreatesDefaultUnusedSummaryRow pins two
// invariants: (1) after UpsertMetricsCatalog, every catalog row has a
// corresponding metrics_usage_summary row with zero counts, even before any
// RefreshMetricsUsageSummary - this lets the unused query INNER JOIN safely
// instead of falling back to a LEFT JOIN + IS NULL branch; and (2) that
// placeholder row must NOT be marked is_unused=TRUE, since a metric that has
// never been evaluated is not the same thing as a metric confirmed to have
// zero usage. See https://github.com/nicolastakashi/prom-analytics-proxy/issues/570.
func TestSQLite_UpsertMetricsCatalog_CreatesDefaultUnusedSummaryRow(t *testing.T) {
	testUpsertMetricsCatalogCreatesDefaultUnusedSummaryRow(t, newTestSQLiteProvider)
}

// TestSQLite_UpsertMetricsCatalog_DuplicateNameInSameCall_LastOccurrenceWins
// verifies a repeated name within one call resolves to its last occurrence's
// values. Unlike PostgreSQL's bulk INSERT ... ON CONFLICT DO UPDATE (which
// errors outright on a duplicate conflict target), SQLite execs one
// prepared statement per item, so this guarantee falls out of exec order
// rather than an explicit de-duplication step - worth pinning directly since
// the two backends reach it by different means.
func TestSQLite_UpsertMetricsCatalog_DuplicateNameInSameCall_LastOccurrenceWins(t *testing.T) {
	p, cleanup := newTestSQLiteProvider(t)
	defer cleanup()

	mustUpsertCatalog(t, p, []MetricCatalogItem{
		{Name: "dup_metric", Type: "gauge", Help: "first"},
		{Name: "other_metric", Type: "counter", Help: "unrelated"},
		{Name: "dup_metric", Type: "counter", Help: "second"},
	})

	var gotType, gotHelp string
	p.WithDB(func(d *sql.DB) {
		err := d.QueryRowContext(context.Background(),
			`SELECT type, help FROM metrics_catalog WHERE name = ?`, "dup_metric").Scan(&gotType, &gotHelp)
		assert.NoError(t, err)
	})
	assert.Equal(t, "counter", gotType, "the later occurrence in the same call must win")
	assert.Equal(t, "second", gotHelp)
}

// TestSQLite_ConcurrentWritesDoNotLoseRows verifies SQLiteProvider's own
// single-writer mutex (mu sync.RWMutex) actually serializes concurrent
// Provider write calls: every item from every concurrent UpsertMetricsCatalog
// call must be persisted, not just some of them silently dropped by a race
// the mutex was supposed to prevent.
func TestSQLite_ConcurrentWritesDoNotLoseRows(t *testing.T) {
	p, cleanup := newTestSQLiteProvider(t)
	defer cleanup()

	const goroutines = 8
	const itemsPerCall = 10

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	ready := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ready
			items := make([]MetricCatalogItem, itemsPerCall)
			for i := range items {
				items[i] = MetricCatalogItem{Name: fmt.Sprintf("concurrent_metric_g%d_i%d", g, i), Type: "gauge", Help: "h"}
			}
			errs <- p.UpsertMetricsCatalog(context.Background(), items)
		}()
	}
	close(ready)
	wg.Wait()
	close(errs)

	for err := range errs {
		assert.NoError(t, err, "concurrent UpsertMetricsCatalog calls must not fail")
	}

	var count int
	p.WithDB(func(d *sql.DB) {
		err := d.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM metrics_catalog WHERE name LIKE 'concurrent_metric_%'`).Scan(&count)
		assert.NoError(t, err)
	})
	assert.Equal(t, goroutines*itemsPerCall, count, "every item from every concurrent call must be persisted")
}

// TestSQLite_GetSeriesMetadataByNames_PopulatesIsUnused guards the write-path
// half of https://github.com/nicolastakashi/prom-analytics-proxy/issues/571:
// GetSeriesMetadataByNames (used by the OTLP ingester's usage-unused lookup)
// must source IsUnused from metrics_usage_summary.is_unused directly, not
// recompute it from the four usage counts - counts alone cannot distinguish
// "evaluated, confirmed zero usage" from "never evaluated yet".
func TestSQLite_GetSeriesMetadataByNames_PopulatesIsUnused(t *testing.T) {
	testGetSeriesMetadataByNamesPopulatesIsUnused(t, newTestSQLiteProvider)
}

// TestSQLite_DashboardUsage verifies dashboard usage time range filtering
func TestSQLite_DashboardUsage(t *testing.T) {
	p, cleanup := newTestSQLiteProvider(t)
	defer cleanup()

	// Use fixed timestamps for predictable testing
	baseTime := time.Date(2025, 8, 18, 20, 0, 0, 0, time.UTC)
	t.Logf("Base time: %v", baseTime.Format(time.RFC3339))
	dashboards := []DashboardUsage{
		{
			Id:        "dash1",
			Serie:     "metric1",
			Name:      "Dashboard 1",
			URL:       "http://grafana/d/dash1",
			CreatedAt: baseTime,
		},
		{
			Id:        "dash2",
			Serie:     "metric1",
			Name:      "Dashboard 2",
			URL:       "http://grafana/d/dash2",
			CreatedAt: baseTime,
		},
		{
			Id:        "dash3",
			Serie:     "metric2", // Different metric
			Name:      "Dashboard 3",
			URL:       "http://grafana/d/dash3",
			CreatedAt: baseTime,
		},
	}

	// Helper function to insert with specific timestamps
	insertWithTime := func(dash DashboardUsage, firstSeen, lastSeen time.Time) error {
		// SQLite doesn't expose first_seen_at/last_seen_at in the struct,
		// but we can set them directly in the database
		var err error
		p.WithDB(func(rawDB *sql.DB) {
			_, err = rawDB.ExecContext(context.Background(), `
				INSERT INTO DashboardUsage (
					id, serie, name, url, created_at, first_seen_at, last_seen_at
				) VALUES (?, ?, ?, ?, datetime(?), datetime(?), datetime(?))
				ON CONFLICT(id, serie) DO UPDATE SET
					last_seen_at = CASE
						WHEN datetime(excluded.last_seen_at) > datetime(last_seen_at) THEN datetime(excluded.last_seen_at)
						ELSE datetime(last_seen_at)
					END`,
				dash.Id, dash.Serie, dash.Name, dash.URL,
				dash.CreatedAt.Format(SQLiteTimeFormat),
				firstSeen.Format(SQLiteTimeFormat),
				lastSeen.Format(SQLiteTimeFormat))
		})
		return err
	}

	// Insert dashboards with specific time ranges
	// dash1: Should not be visible in time range filter test (18:30-20:00)
	err := insertWithTime(dashboards[0], baseTime.Add(-2*time.Hour), baseTime.Add(-2*time.Hour))
	assert.NoError(t, err, "Failed to insert dash1")
	// dash2: Should be visible in time range filter test (18:30-20:00)
	err = insertWithTime(dashboards[1], baseTime.Add(-30*time.Minute), baseTime)
	assert.NoError(t, err, "Failed to insert dash2")
	// dash3: Different metric
	err = insertWithTime(dashboards[2], baseTime.Add(-30*time.Minute), baseTime)
	assert.NoError(t, err, "Failed to insert dash3")

	tests := []struct {
		name    string
		params  DashboardUsageParams
		wantLen int
		wantIDs []string
		wantErr bool
	}{
		{
			name: "Find all dashboards for metric1",
			params: DashboardUsageParams{
				Serie: "metric1",
				TimeRange: TimeRange{
					From: baseTime.Add(-3 * time.Hour),
					To:   baseTime,
				},
				Page:     1,
				PageSize: 10,
			},
			wantLen: 2,
			wantIDs: []string{"dash1", "dash2"},
		},
		{
			name: "Find dashboards with time range filter",
			params: DashboardUsageParams{
				Serie: "metric1",
				TimeRange: TimeRange{
					From: baseTime.Add(-90 * time.Minute),
					To:   baseTime,
				},
				Page:     1,
				PageSize: 10,
			},
			wantLen: 1,
			wantIDs: []string{"dash2"},
		},
		{
			name: "Find dashboards with name filter",
			params: DashboardUsageParams{
				Serie: "metric1",
				TimeRange: TimeRange{
					From: baseTime.Add(-3 * time.Hour),
					To:   baseTime,
				},
				Filter:   "Dashboard 1",
				Page:     1,
				PageSize: 10,
			},
			wantLen: 1,
			wantIDs: []string{"dash1"},
		},
		{
			name: "No dashboards for non-existent metric",
			params: DashboardUsageParams{
				Serie: "non-existent",
				TimeRange: TimeRange{
					From: baseTime.Add(-3 * time.Hour),
					To:   baseTime,
				},
				Page:     1,
				PageSize: 10,
			},
			wantLen: 0,
			wantIDs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Test case %s: time range from=%v to=%v",
				tt.name,
				tt.params.TimeRange.From.Format(time.RFC3339),
				tt.params.TimeRange.To.Format(time.RFC3339))
			got, err := p.GetDashboardUsage(context.Background(), tt.params)
			if tt.wantErr {
				assert.Error(t, err, "GetDashboardUsage() should return error")
				return
			}
			assert.NoError(t, err, "GetDashboardUsage() should not return error")

			results, ok := got.Data.([]DashboardUsage)
			assert.True(t, ok, "Expected Data to be []DashboardUsage, got %T", got.Data)

			assert.Equal(t, tt.wantLen, len(results), "GetDashboardUsage() result count")

			// Verify we got the expected dashboard IDs
			gotIDs := make([]string, len(results))
			for i, r := range results {
				gotIDs[i] = r.Id
			}

			// Create maps for easier comparison
			gotIDMap := make(map[string]bool)
			for _, id := range gotIDs {
				gotIDMap[id] = true
			}
			for _, wantID := range tt.wantIDs {
				assert.True(t, gotIDMap[wantID], "GetDashboardUsage() missing expected ID %s", wantID)
			}
		})
	}
}

// TestSQLite_QueryTimeRangeDistribution verifies bucketed counts and percents for range queries
func TestSQLite_QueryTimeRangeDistribution(t *testing.T) {
	testQueryTimeRangeDistribution(t, newTestSQLiteProvider)
}

func TestSQLite_TimeRangeDistribution_ISO_TZ(t *testing.T) {
	p, cleanup := newTestSQLiteProvider(t)
	defer cleanup()
	now := time.Now().UTC().Truncate(time.Minute)
	from := now.Add(-15 * time.Minute)

	insert := `INSERT INTO queries (ts, queryParam, timeParam, duration, statusCode, bodySize, fingerprint, labelMatchers, type, step, start, "end", totalQueryableSamples, peakSamples)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	// Three ranges: 5m, 2h, 9d
	ranges := []struct{ start, end time.Time }{
		{now.Add(-10 * time.Minute), now.Add(-5 * time.Minute)},
		{now.Add(-3 * time.Hour), now.Add(-1 * time.Hour)},
		{now.Add(-9 * 24 * time.Hour), now},
	}
	p.WithDB(func(rawDB *sql.DB) {
		// Manually insert a few range queries with ISO timestamps (T/Z) to simulate proxy inserts
		_, _ = rawDB.ExecContext(context.Background(), `DELETE FROM queries`)

		for _, r := range ranges {
			_, err := rawDB.ExecContext(context.Background(), insert,
				now.Format(time.RFC3339), // ts
				"up",                     // queryParam
				now.Format(time.RFC3339), // timeParam
				int64(100),               // duration ms
				200,                      // statusCode
				0,                        // bodySize
				"fp",                     // fingerprint
				`[{"__name__":"up"}]`,    // labelMatchers
				"range",                  // type
				15.0,                     // step
				r.start.Format(time.RFC3339),
				r.end.Format(time.RFC3339),
				0,
				0,
			)
			assert.NoError(t, err, "insert")
		}
	})

	out, err := p.GetQueryTimeRangeDistribution(context.Background(), TimeRange{From: from, To: now}, "")
	assert.NoError(t, err, "GetQueryTimeRangeDistribution")
	assert.NotEmpty(t, out, "no buckets returned")

	// Sum should be 3
	sum := 0
	for _, b := range out {
		sum += b.Count
	}
	assert.Greater(t, sum, 0, "expected non-zero distribution")
}

func TestSQLiteProvider_DeleteQueriesBefore(t *testing.T) {
	p, cleanup := newTestSQLiteProvider(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	cutoff := now.Add(-1 * time.Hour)

	insert := `INSERT INTO queries (ts, queryParam, timeParam, duration, statusCode, bodySize, fingerprint, labelMatchers, type, step, start, "end", totalQueryableSamples, peakSamples)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	// Seeding and the pre-deletion count share one WithDB call; the
	// post-deletion counts get their own, separate call after
	// p.DeleteQueriesBefore returns - nesting a Provider write inside a
	// WithDB callback would deadlock: WithDB holds a non-reentrant RLock
	// for the callback's duration, and DeleteQueriesBefore needs the
	// provider's own write lock.
	p.WithDB(func(rawDB *sql.DB) {
		for i := 0; i < 3; i++ {
			ts := cutoff.Add(-time.Duration(i+1) * time.Hour)
			_, err := rawDB.ExecContext(ctx, insert,
				ts.Format(time.RFC3339), "query1", now.Format(time.RFC3339), int64(100), 200, 0, "fp1", `[{"__name__":"up"}]`, "instant", 0.0, time.Time{}, time.Time{}, 0, 0,
			)
			assert.NoError(t, err, "insert old query")
		}

		for i := 0; i < 2; i++ {
			ts := cutoff.Add(time.Duration(i+1) * time.Hour)
			_, err := rawDB.ExecContext(ctx, insert,
				ts.Format(time.RFC3339), "query2", now.Format(time.RFC3339), int64(100), 200, 0, "fp2", `[{"__name__":"up"}]`, "instant", 0.0, time.Time{}, time.Time{}, 0, 0,
			)
			assert.NoError(t, err, "insert new query")
		}

		var count int
		err := rawDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM queries").Scan(&count)
		assert.NoError(t, err, "count before deletion")
		assert.Equal(t, 5, count, "should have 5 queries initially")
	})

	deleted, err := p.DeleteQueriesBefore(ctx, cutoff)
	assert.NoError(t, err, "DeleteQueriesBefore")
	assert.Equal(t, int64(3), deleted, "should delete 3 queries")

	p.WithDB(func(rawDB *sql.DB) {
		var count int
		err := rawDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM queries").Scan(&count)
		assert.NoError(t, err, "count after deletion")
		assert.Equal(t, 2, count, "should have 2 queries remaining")

		var remainingCount int
		err = rawDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM queries WHERE ts >= datetime(?)", cutoff.Format(time.RFC3339)).Scan(&remainingCount)
		assert.NoError(t, err, "count queries after cutoff")
		assert.Equal(t, 2, remainingCount, "all remaining queries should be after cutoff")
	})

	deleted2, err := p.DeleteQueriesBefore(ctx, cutoff)
	assert.NoError(t, err, "DeleteQueriesBefore second call")
	assert.Equal(t, int64(0), deleted2, "should delete 0 queries on second call")
}

func TestSQLite_WriteMethodsFailCleanlyOnCancelledContext(t *testing.T) {
	testWriteMethodsFailCleanlyOnCancelledContext(t, newTestSQLiteProvider)
}

func TestSQLite_WriteMethodsNoOpOnEmptyInput(t *testing.T) {
	testWriteMethodsNoOpOnEmptyInput(t, newTestSQLiteProvider)
}
