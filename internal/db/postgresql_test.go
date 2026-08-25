package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/nicolastakashi/prom-analytics-proxy/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// newRawPostgresContainer spins up a disposable PostgreSQL container and
// returns its host/port and a terminate func, skipping the test if Docker
// isn't available. For the common case of just wanting a ready-to-use
// Provider, use newTestPostgreSQLProvider instead - this exists for callers
// that need the raw connection first, to do something NewPostgreSQLProvider
// itself can't (bootstrap a non-default TimeZone, pass a custom
// StatementTimeout on construction).
func newRawPostgresContainer(t *testing.T) (host string, port int, terminate func()) {
	t.Helper()

	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx, "postgres:16",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		// Docker not available in this environment; skip tests gracefully
		t.Skipf("Skipping PostgreSQL container tests (Docker not available): %v", err)
	}

	h, err := pgContainer.Host(ctx)
	require.NoError(t, err, "container host")
	mappedPort, err := pgContainer.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err, "container port")
	portNum, err := strconv.Atoi(mappedPort.Port())
	require.NoError(t, err, "container port number")

	return h, portNum, func() { _ = pgContainer.Terminate(ctx) }
}

// newTestPostgreSQLProvider spins up a disposable PostgreSQL using Testcontainers
// and returns a configured Provider and a cleanup function.
func newTestPostgreSQLProvider(t *testing.T) (Provider, func()) {
	t.Helper()

	host, portNum, terminate := newRawPostgresContainer(t)

	p, err := NewPostgreSQLProvider(context.Background(), config.PostgreSQLConfig{
		Addr:        host,
		Port:        portNum,
		User:        "testuser",
		Password:    "testpass",
		Database:    "testdb",
		SSLMode:     "disable",
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		terminate()
		require.NoError(t, err, "failed to init postgres provider")
	}

	return p, func() {
		_ = p.Close()
		terminate()
	}
}

// assertConcurrentOverlappingUpsertsDoNotDeadlock races two concurrent
// calls to upsert - each given the same itemsPerCall items, built by
// buildItem, but in exactly reversed relative order - and asserts neither
// ever fails. Two callers upserting overlapping rows via ON CONFLICT DO
// UPDATE in opposite orders is Postgres's own documented deadlock
// precondition (see
// https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-DEADLOCKS):
// "the best defense against deadlocks is generally to avoid them by being
// certain that all applications ... acquire locks ... in a consistent
// order."
//
// This repeats the attempt many times: a deadlock only happens if the two
// calls' row-lock acquisition genuinely overlaps in time, which goroutine
// scheduling doesn't guarantee on any single attempt - one try could pass
// by luck even against code that doesn't sort before upserting.
func assertConcurrentOverlappingUpsertsDoNotDeadlock[T any](
	t *testing.T,
	itemsPerCall, attempts int,
	buildItem func(i int) T,
	upsert func(context.Context, []T) error,
) {
	t.Helper()

	ascending := make([]T, itemsPerCall)
	descending := make([]T, itemsPerCall)
	for i := 0; i < itemsPerCall; i++ {
		item := buildItem(i)
		ascending[i] = item
		descending[itemsPerCall-1-i] = item
	}

	for attempt := 0; attempt < attempts; attempt++ {
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		ready := make(chan struct{})

		for _, items := range [][]T{ascending, descending} {
			items := items
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-ready
				errs <- upsert(context.Background(), items)
			}()
		}
		close(ready)
		wg.Wait()
		close(errs)

		for err := range errs {
			assert.NoError(t, err, "attempt %d: concurrent overlapping upserts must not fail", attempt)
		}
	}
}

// TestNewPostgreSQLProvider_DoesNotMutateGlobalConfig verifies that
// NewPostgreSQLProvider uses only the supplied config and leaves
// config.DefaultConfig untouched.
func TestNewPostgreSQLProvider_DoesNotMutateGlobalConfig(t *testing.T) {
	t.Parallel()
	prov, cleanup := newTestPostgreSQLProvider(t)
	defer cleanup()

	before := config.DefaultConfig.Database
	// Round-trip a trivial query to confirm the provider is functional.
	_, err := prov.ListJobs(context.Background())
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if config.DefaultConfig.Database != before {
		t.Fatal("NewPostgreSQLProvider must not mutate config.DefaultConfig")
	}
}

func TestPostgreSQL_GetQueryTypes(t *testing.T) {
	t.Parallel()
	testGetQueryTypes(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_GetAverageDuration(t *testing.T) {
	t.Parallel()
	testGetAverageDuration(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_GetQueryRate(t *testing.T) {
	t.Parallel()
	testGetQueryRate(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_GetQueryLatencyTrends_And_Throughput_And_Errors(t *testing.T) {
	t.Parallel()
	testGetQueryLatencyTrendsAndThroughputAndErrors(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_AnalyticsMethodsOnEmptyDatabase(t *testing.T) {
	t.Parallel()
	testAnalyticsMethodsOnEmptyDatabase(t, newTestPostgreSQLProvider)
}

// -------------------- Aggregations --------------------

func TestPostgreSQL_GetQueriesBySerieName(t *testing.T) {
	t.Parallel()
	testGetQueriesBySerieName(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_GetQueriesBySerieName_Pagination(t *testing.T) {
	t.Parallel()
	testGetQueriesBySerieNamePagination(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_GetQueryExpressions_And_Executions(t *testing.T) {
	t.Parallel()
	testGetQueryExpressionsAndExecutions(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_GetQueryExpressions_Pagination(t *testing.T) {
	t.Parallel()
	testGetQueryExpressionsPagination(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_GetQueryExecutions_Pagination_TypeFilter_And_HTTPHeaders(t *testing.T) {
	t.Parallel()
	testGetQueryExecutionsPaginationTypeFilterAndHTTPHeaders(t, newTestPostgreSQLProvider)
}

// -------------------- Metrics Inventory --------------------

func TestPostgreSQL_MetricsJobIndex_And_ListJobs(t *testing.T) {
	t.Parallel()
	testMetricsJobIndexAndListJobs(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_RefreshMetricsUsageSummary_And_GetSeriesMetadata(t *testing.T) {
	t.Parallel()
	testRefreshMetricsUsageSummaryAndGetSeriesMetadata(t, newTestPostgreSQLProvider)
}

// mustSummaryRowPostgreSQL reads back one metrics_usage_summary row's four
// usage counts plus is_unused. Callers assert on the whole row rather than a
// single count: is_unused is derived from all four, so seeing the whole row
// is what tells you which one is off if this ever fails again.
func mustSummaryRowPostgreSQL(t *testing.T, p Provider, name string) summaryRow {
	t.Helper()
	var r summaryRow
	p.WithDB(func(d *sql.DB) {
		row := d.QueryRowContext(context.Background(),
			`SELECT alert_count, record_count, dashboard_count, query_count, is_unused FROM metrics_usage_summary WHERE name = $1`, name)
		require.NoError(t, row.Scan(&r.Alert, &r.Record, &r.Dashboard, &r.Query, &r.Unused))
	})
	return r
}

// TestPostgreSQL_RefreshMetricsUsageSummary_ExcludesStaleCatalogRows is the
// PostgreSQL counterpart of TestSQLite_RefreshMetricsUsageSummary_ExcludesStaleCatalogRows:
// see there for the full rationale.
func TestPostgreSQL_RefreshMetricsUsageSummary_ExcludesStaleCatalogRows(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
	defer cleanup()

	mustUpsertCatalog(t, p, []MetricCatalogItem{
		{Name: "fresh_metric", Type: "gauge", Help: "still scraped"},
		{Name: "stale_metric", Type: "gauge", Help: "no longer scraped"},
	})

	p.WithDB(func(d *sql.DB) {
		_, err := d.ExecContext(context.Background(),
			`UPDATE metrics_catalog SET last_synced_at = NOW() - INTERVAL '90 days' WHERE name = $1`, "stale_metric")
		assert.NoError(t, err, "backdate stale_metric")
	})

	now := time.Now().UTC()
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

	fresh := mustSummaryRowPostgreSQL(t, p, "fresh_metric")
	assert.Greater(t, fresh.Query, 0, "fresh_metric should have been recomputed with real usage")
	assert.False(t, fresh.Unused, "fresh_metric should be marked used")

	stale := mustSummaryRowPostgreSQL(t, p, "stale_metric")
	assert.Equal(t, summaryRow{Alert: 0, Record: 0, Dashboard: 0, Query: 0, Unused: false}, stale,
		"stale_metric's summary should remain untouched at its placeholder values - got %+v", stale)
}

// TestPostgreSQL_RefreshMetricsUsageSummary_ExcludesOutOfWindowRulesUsage guards
// RulesUsage's own presence-window filter in RefreshMetricsUsageSummary: a rule
// whose first_seen_at/last_seen_at fall outside the refresh's TimeRange must not
// count toward alert_count/record_count. See
// https://github.com/nicolastakashi/prom-analytics-proxy/issues/589.
func TestPostgreSQL_RefreshMetricsUsageSummary_ExcludesOutOfWindowRulesUsage(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
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
			`UPDATE RulesUsage SET first_seen_at = NOW() - INTERVAL '100 days', last_seen_at = NOW() - INTERVAL '99 days' WHERE serie = $1`,
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

	inWindow := mustSummaryRowPostgreSQL(t, p, "in_window_metric")
	assert.Equal(t, summaryRow{Alert: 1, Record: 0, Dashboard: 0, Query: 0, Unused: false}, inWindow,
		"in_window_metric's alert rule falls inside the refresh window and must be counted - got %+v", inWindow)

	outOfWindow := mustSummaryRowPostgreSQL(t, p, "out_of_window_metric")
	assert.Equal(t, summaryRow{Alert: 0, Record: 0, Dashboard: 0, Query: 0, Unused: true}, outOfWindow,
		"out_of_window_metric's rule presence ended before the refresh window started, so it must not be counted as used - got %+v", outOfWindow)
}

// TestPostgreSQL_UpsertMetricsCatalog_LastSyncedAtIsUTC guards last_synced_at
// staying in the same clock domain as the $1 bound RefreshMetricsUsageSummary
// compares it against (tr.From.UTC(), via PrepareTimeRange): last_synced_at is
// TIMESTAMP WITHOUT TIME ZONE, so a bare NOW() lands in the writing session's
// TimeZone instead of UTC, silently turning the freshness filter into a
// permanent no-op on any server whose TimeZone isn't UTC. This container's own
// session already defaults to TimeZone=UTC - which would let the bug pass
// vacuously - so the database default is pinned to a fixed, DST-free non-UTC
// offset before the provider (and its connection pool) ever connects.
func TestPostgreSQL_UpsertMetricsCatalog_LastSyncedAtIsUTC(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	host, portNum, terminate := newRawPostgresContainer(t)
	defer terminate()

	bootstrapDSN := fmt.Sprintf(
		"host='%s' port=%d user='testuser' password='testpass' dbname='testdb' sslmode='disable'",
		host, portNum,
	)
	bootstrap, err := sql.Open("postgres", bootstrapDSN)
	require.NoError(t, err, "open bootstrap connection")
	_, err = bootstrap.ExecContext(ctx, `ALTER DATABASE testdb SET TIME ZONE 'Etc/GMT+5'`)
	require.NoError(t, err, "pin database default TimeZone to a fixed non-UTC offset")
	require.NoError(t, bootstrap.Close())

	p, err := NewPostgreSQLProvider(ctx, config.PostgreSQLConfig{
		Addr:        host,
		Port:        portNum,
		User:        "testuser",
		Password:    "testpass",
		Database:    "testdb",
		SSLMode:     "disable",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err, "failed to init postgres provider")
	defer func() { _ = p.Close() }()

	require.NoError(t, p.UpsertMetricsCatalog(ctx, []MetricCatalogItem{{Name: "tz_metric", Type: "gauge", Help: "h"}}))

	var lastSynced time.Time
	p.WithDB(func(d *sql.DB) {
		row := d.QueryRowContext(ctx, `SELECT last_synced_at FROM metrics_catalog WHERE name = $1`, "tz_metric")
		require.NoError(t, row.Scan(&lastSynced), "scan last_synced_at")
	})

	assert.WithinDuration(t, time.Now().UTC(), lastSynced, 10*time.Second,
		"last_synced_at must be written in UTC regardless of the writing session's TimeZone (pinned to Etc/GMT+5 here); "+
			"a bare NOW() would drift by the session's offset instead")
}

func TestPostgreSQL_GetMetricStatistics_And_QueryPerformanceStats(t *testing.T) {
	t.Parallel()
	testGetMetricStatisticsAndQueryPerformanceStats(t, newTestPostgreSQLProvider)
}

// -------------------- Rules & Dashboards --------------------

func TestPostgreSQL_InsertRulesUsage_GetRulesUsage(t *testing.T) {
	t.Parallel()
	testInsertRulesUsageGetRulesUsage(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_GetRulesUsage_SortFieldsAndPagination(t *testing.T) {
	t.Parallel()
	testGetRulesUsageSortFieldsAndPagination(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_InsertDashboardUsage_UpsertBehavior(t *testing.T) {
	t.Parallel()
	testInsertDashboardUsageUpsertBehavior(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_GetDashboardUsage_SortFieldsAndPagination(t *testing.T) {
	t.Parallel()
	testGetDashboardUsageSortFieldsAndPagination(t, newTestPostgreSQLProvider)
}

// TestPostgreSQL_GetRulesUsage_MaliciousSortOrderDoesNotBreakQuery guards
// GetRulesUsage's ORDER BY clause: an unvalidated SortOrder must not become
// a SQL injection vector, and must instead fall back to a safe default
// order, like every other paginated method already routed through
// ValidateSortField's whitelist.
func TestPostgreSQL_GetRulesUsage_MaliciousSortOrderDoesNotBreakQuery(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
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

// TestPostgreSQL_GetDashboardUsage_MaliciousSortOrderDoesNotBreakQuery is
// TestPostgreSQL_GetRulesUsage_MaliciousSortOrderDoesNotBreakQuery's
// counterpart for GetDashboardUsage, guarding the same injection class.
func TestPostgreSQL_GetDashboardUsage_MaliciousSortOrderDoesNotBreakQuery(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
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

// TestPostgreSQL_InsertRulesUsage_ConcurrentOverlappingUpsertsDoNotDeadlock
// verifies InsertRulesUsage tolerates concurrent calls upserting
// overlapping rows in different orders without deadlocking.
func TestPostgreSQL_InsertRulesUsage_ConcurrentOverlappingUpsertsDoNotDeadlock(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
	defer cleanup()

	assertConcurrentOverlappingUpsertsDoNotDeadlock(t, 40, 25,
		func(i int) RulesUsage {
			return RulesUsage{
				Serie:     fmt.Sprintf("deadlock_serie_%02d", i),
				GroupName: "g", Name: "r", Expression: "e", Kind: string(RuleUsageKindAlert),
			}
		},
		p.InsertRulesUsage,
	)
}

// TestPostgreSQL_InsertDashboardUsage_ConcurrentOverlappingUpsertsDoNotDeadlock
// verifies InsertDashboardUsage tolerates concurrent calls upserting
// overlapping rows in different orders without deadlocking.
func TestPostgreSQL_InsertDashboardUsage_ConcurrentOverlappingUpsertsDoNotDeadlock(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
	defer cleanup()

	assertConcurrentOverlappingUpsertsDoNotDeadlock(t, 40, 25,
		func(i int) DashboardUsage {
			return DashboardUsage{Id: fmt.Sprintf("deadlock_dash_%02d", i), Serie: "m", Name: "n", URL: "u"}
		},
		p.InsertDashboardUsage,
	)
}

// -------------------- Metrics catalog / inventory / usage --------------------

func TestPostgreSQL_HistogramSummaryMetricsCatalog(t *testing.T) {
	t.Parallel()
	testHistogramSummaryMetricsCatalog(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_MetricsInventoryAndList(t *testing.T) {
	t.Parallel()
	testMetricsInventoryAndList(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_GetSeriesMetadata_UsageFilters(t *testing.T) {
	t.Parallel()
	testGetSeriesMetadataUsageFilters(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_GetSeriesMetadata_EmptyResults(t *testing.T) {
	t.Parallel()
	testGetSeriesMetadataEmptyResults(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_GetSeriesMetadataUnusedJobScoped(t *testing.T) {
	t.Parallel()
	testGetSeriesMetadataUnusedJobScoped(t, newTestPostgreSQLProvider)
}

// TestPostgreSQL_UpsertMetricsCatalog_CreatesDefaultUnusedSummaryRow pins two
// invariants: (1) after UpsertMetricsCatalog, every catalog row has a
// corresponding metrics_usage_summary row with zero counts, even before any
// RefreshMetricsUsageSummary - this lets the unused query INNER JOIN safely
// instead of falling back to a LEFT JOIN + IS NULL branch; and (2) that
// placeholder row must NOT be marked is_unused=TRUE, since a metric that has
// never been evaluated is not the same thing as a metric confirmed to have
// zero usage. See https://github.com/nicolastakashi/prom-analytics-proxy/issues/570.
func TestPostgreSQL_UpsertMetricsCatalog_CreatesDefaultUnusedSummaryRow(t *testing.T) {
	t.Parallel()
	testUpsertMetricsCatalogCreatesDefaultUnusedSummaryRow(t, newTestPostgreSQLProvider)
}

// TestPostgreSQL_UpsertMetricsCatalog_ConcurrentOverlappingUpsertsDoNotDeadlock
// verifies UpsertMetricsCatalog tolerates concurrent calls upserting
// overlapping rows in different orders without deadlocking.
func TestPostgreSQL_UpsertMetricsCatalog_ConcurrentOverlappingUpsertsDoNotDeadlock(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
	defer cleanup()

	assertConcurrentOverlappingUpsertsDoNotDeadlock(t, 40, 25,
		func(i int) MetricCatalogItem {
			return MetricCatalogItem{Name: fmt.Sprintf("deadlock_metric_%02d", i), Type: "gauge", Help: "h"}
		},
		p.UpsertMetricsCatalog,
	)
}

// TestPostgreSQL_UpsertMetricsCatalog_DuplicateNameInSameCall_LastOccurrenceWins
// verifies a repeated name within one call resolves to its last
// occurrence's values. This must keep holding with a bulk
// INSERT ... ON CONFLICT DO UPDATE statement, which errors outright if
// its own input contains the same conflict target twice ("ON CONFLICT DO
// UPDATE command cannot affect row a second time"), so de-duplicating
// before upserting is required independently of the deadlock fix itself.
func TestPostgreSQL_UpsertMetricsCatalog_DuplicateNameInSameCall_LastOccurrenceWins(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
	defer cleanup()

	mustUpsertCatalog(t, p, []MetricCatalogItem{
		{Name: "dup_metric", Type: "gauge", Help: "first"},
		{Name: "other_metric", Type: "counter", Help: "unrelated"},
		{Name: "dup_metric", Type: "counter", Help: "second"},
	})

	var gotType, gotHelp string
	p.WithDB(func(d *sql.DB) {
		err := d.QueryRowContext(context.Background(),
			`SELECT type, help FROM metrics_catalog WHERE name = $1`, "dup_metric").Scan(&gotType, &gotHelp)
		assert.NoError(t, err)
	})
	assert.Equal(t, "counter", gotType, "the later occurrence in the same call must win")
	assert.Equal(t, "second", gotHelp)
}

// TestPostgreSQL_UpsertMetricsCatalog_ManyRows_EachRowGetsItsOwnValues
// verifies each row in a multi-row call gets its own field values, not
// another row's. This guards against a risk specific to a bulk-unnest
// statement: four parallel arrays (name/type/help/unit) are passed
// positionally into unnest($1, $2, $3, $4), and a swap (e.g. passing
// helps where types belongs) would compile fine and silently misfile
// every row's fields.
func TestPostgreSQL_UpsertMetricsCatalog_ManyRows_EachRowGetsItsOwnValues(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
	defer cleanup()

	items := []MetricCatalogItem{
		{Name: "metric_alpha", Type: "gauge", Help: "help alpha", Unit: "bytes"},
		{Name: "metric_beta", Type: "counter", Help: "help beta", Unit: "seconds"},
		{Name: "metric_gamma", Type: "histogram", Help: "help gamma", Unit: "requests"},
	}
	mustUpsertCatalog(t, p, items)

	p.WithDB(func(d *sql.DB) {
		for _, want := range items {
			var gotType, gotHelp, gotUnit string
			err := d.QueryRowContext(context.Background(),
				`SELECT type, help, unit FROM metrics_catalog WHERE name = $1`, want.Name).
				Scan(&gotType, &gotHelp, &gotUnit)
			assert.NoError(t, err, "row for %s", want.Name)
			assert.Equal(t, want.Type, gotType, "%s: type", want.Name)
			assert.Equal(t, want.Help, gotHelp, "%s: help", want.Name)
			assert.Equal(t, want.Unit, gotUnit, "%s: unit", want.Name)
		}
	})
}

// TestPostgreSQL_UpsertMetricsJobIndex_ConcurrentOverlappingUpsertsDoNotDeadlock
// verifies UpsertMetricsJobIndex tolerates concurrent calls upserting
// overlapping rows in different orders without deadlocking.
func TestPostgreSQL_UpsertMetricsJobIndex_ConcurrentOverlappingUpsertsDoNotDeadlock(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
	defer cleanup()

	assertConcurrentOverlappingUpsertsDoNotDeadlock(t, 40, 25,
		func(i int) MetricJobIndexItem {
			return MetricJobIndexItem{Name: fmt.Sprintf("deadlock_metric_%02d", i), Job: "shared_job"}
		},
		p.UpsertMetricsJobIndex,
	)
}

// TestPostgreSQL_GetSeriesMetadataByNames_PopulatesIsUnused guards the
// write-path half of https://github.com/nicolastakashi/prom-analytics-proxy/issues/571:
// GetSeriesMetadataByNames (used by the OTLP ingester's usage-unused lookup)
// must source IsUnused from metrics_usage_summary.is_unused directly, not
// recompute it from the four usage counts - counts alone cannot distinguish
// "evaluated, confirmed zero usage" from "never evaluated yet".
func TestPostgreSQL_GetSeriesMetadataByNames_PopulatesIsUnused(t *testing.T) {
	t.Parallel()
	testGetSeriesMetadataByNamesPopulatesIsUnused(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_DashboardUsage(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
	defer cleanup()

	baseTime := time.Date(2025, 8, 18, 20, 0, 0, 0, time.UTC)
	dashboards := []DashboardUsage{
		{Id: "dash1", Serie: "metric1", Name: "Dashboard 1", URL: "http://grafana/d/dash1", CreatedAt: baseTime},
		{Id: "dash2", Serie: "metric1", Name: "Dashboard 2", URL: "http://grafana/d/dash2", CreatedAt: baseTime},
		{Id: "dash3", Serie: "metric2", Name: "Dashboard 3", URL: "http://grafana/d/dash3", CreatedAt: baseTime},
	}

	// Insert with specific first_seen_at / last_seen_at via direct SQL
	insertWithTime := func(dash DashboardUsage, firstSeen, lastSeen time.Time) error {
		var err error
		p.WithDB(func(rawDB *sql.DB) {
			_, err = rawDB.ExecContext(context.Background(), `
				INSERT INTO DashboardUsage (id, serie, name, url, created_at, first_seen_at, last_seen_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
				ON CONFLICT (id, serie) DO UPDATE SET
					last_seen_at = CASE
						WHEN EXCLUDED.last_seen_at > DashboardUsage.last_seen_at THEN EXCLUDED.last_seen_at
						ELSE DashboardUsage.last_seen_at
					END`,
				dash.Id, dash.Serie, dash.Name, dash.URL,
				dash.CreatedAt,
				firstSeen,
				lastSeen,
			)
		})
		return err
	}

	err := insertWithTime(dashboards[0], baseTime.Add(-2*time.Hour), baseTime.Add(-2*time.Hour))
	assert.NoError(t, err, "Failed to insert dash1")
	err = insertWithTime(dashboards[1], baseTime.Add(-30*time.Minute), baseTime)
	assert.NoError(t, err, "Failed to insert dash2")
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
				Serie:     "metric1",
				TimeRange: TimeRange{From: baseTime.Add(-3 * time.Hour), To: baseTime},
				Page:      1, PageSize: 10,
			},
			wantLen: 2,
			wantIDs: []string{"dash1", "dash2"},
		},
		{
			name: "Find dashboards with time range filter",
			params: DashboardUsageParams{
				Serie:     "metric1",
				TimeRange: TimeRange{From: baseTime.Add(-90 * time.Minute), To: baseTime},
				Page:      1, PageSize: 10,
			},
			wantLen: 1,
			wantIDs: []string{"dash2"},
		},
		{
			name: "Find dashboards with name filter",
			params: DashboardUsageParams{
				Serie:     "metric1",
				TimeRange: TimeRange{From: baseTime.Add(-3 * time.Hour), To: baseTime},
				Filter:    "Dashboard 1",
				Page:      1, PageSize: 10,
			},
			wantLen: 1,
			wantIDs: []string{"dash1"},
		},
		{
			name: "No dashboards for non-existent metric",
			params: DashboardUsageParams{
				Serie:     "non-existent",
				TimeRange: TimeRange{From: baseTime.Add(-3 * time.Hour), To: baseTime},
				Page:      1, PageSize: 10,
			},
			wantLen: 0,
			wantIDs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.GetDashboardUsage(context.Background(), tt.params)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			results, ok := got.Data.([]DashboardUsage)
			assert.True(t, ok, "Expected Data to be []DashboardUsage, got %T", got.Data)
			assert.Equal(t, tt.wantLen, len(results))

			gotIDs := make(map[string]bool)
			for _, r := range results {
				gotIDs[r.Id] = true
			}
			for _, wantID := range tt.wantIDs {
				assert.True(t, gotIDs[wantID], "missing expected ID %s", wantID)
			}
		})
	}
}

func TestPostgreSQL_QueryTimeRangeDistribution(t *testing.T) {
	t.Parallel()
	testQueryTimeRangeDistribution(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_TimeRangeDistribution_ISO_TZ(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
	defer cleanup()
	now := time.Now().UTC().Truncate(time.Minute)
	from := now.Add(-15 * time.Minute)

	insert := `INSERT INTO queries (ts, queryParam, timeParam, duration, statusCode, bodySize, fingerprint, labelMatchers, type, step, start, "end", totalQueryableSamples, peakSamples)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13, $14)`

	ranges := []struct{ start, end time.Time }{
		{now.Add(-10 * time.Minute), now.Add(-5 * time.Minute)},
		{now.Add(-3 * time.Hour), now.Add(-1 * time.Hour)},
		{now.Add(-9 * 24 * time.Hour), now},
	}
	p.WithDB(func(rawDB *sql.DB) {
		_, _ = rawDB.ExecContext(context.Background(), `DELETE FROM queries`)

		for _, r := range ranges {
			_, err := rawDB.ExecContext(context.Background(), insert,
				now,
				"up",
				now,
				int64(100),
				200,
				0,
				"fp",
				`[{"__name__":"up"}]`,
				"range",
				15.0,
				r.start,
				r.end,
				0,
				0,
			)
			assert.NoError(t, err, "insert")
		}
	})

	out, err := p.GetQueryTimeRangeDistribution(context.Background(), TimeRange{From: from, To: now}, "")
	assert.NoError(t, err, "GetQueryTimeRangeDistribution")
	assert.NotEmpty(t, out)
}

func TestPostgreSQLProvider_DeleteQueriesBefore(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
	defer cleanup()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	cutoff := now.Add(-1 * time.Hour)

	insert := `INSERT INTO queries (ts, queryParam, timeParam, duration, statusCode, bodySize, fingerprint, labelMatchers, type, step, start, "end", totalQueryableSamples, peakSamples)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13, $14)`

	// Seeding and the pre-deletion count share one WithDB call; the post-
	// deletion counts get their own, separate call after
	// p.DeleteQueriesBefore returns - nesting a Provider write inside a
	// WithDB callback risks deadlock if WithDB holds a lock across the
	// callback.
	p.WithDB(func(rawDB *sql.DB) {
		for i := 0; i < 3; i++ {
			ts := cutoff.Add(-time.Duration(i+1) * time.Hour)
			_, err := rawDB.ExecContext(ctx, insert,
				ts, "query1", now, int64(100), 200, 0, "fp1", `[{"__name__":"up"}]`, "instant", 0.0, time.Time{}, time.Time{}, 0, 0,
			)
			assert.NoError(t, err, "insert old query")
		}

		for i := 0; i < 2; i++ {
			ts := cutoff.Add(time.Duration(i+1) * time.Hour)
			_, err := rawDB.ExecContext(ctx, insert,
				ts, "query2", now, int64(100), 200, 0, "fp2", `[{"__name__":"up"}]`, "instant", 0.0, time.Time{}, time.Time{}, 0, 0,
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
		err = rawDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM queries WHERE ts >= $1", cutoff).Scan(&remainingCount)
		assert.NoError(t, err, "count queries after cutoff")
		assert.Equal(t, 2, remainingCount, "all remaining queries should be after cutoff")
	})
}

func TestPostgreSQL_StatementTimeoutAborts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	host, portNum, terminate := newRawPostgresContainer(t)
	defer terminate()

	p, err := NewPostgreSQLProvider(ctx, config.PostgreSQLConfig{
		Addr:             host,
		Port:             portNum,
		User:             "testuser",
		Password:         "testpass",
		Database:         "testdb",
		SSLMode:          "disable",
		DialTimeout:      5 * time.Second,
		StatementTimeout: 500 * time.Millisecond,
	})
	require.NoError(t, err, "failed to init postgres provider")
	defer func() { _ = p.Close() }()

	p.WithDB(func(rawDB *sql.DB) {
		// Sanity: a fast query still succeeds.
		var fast int
		err := rawDB.QueryRowContext(ctx, "SELECT 1").Scan(&fast)
		assert.NoError(t, err, "fast query under the budget should succeed")
		assert.Equal(t, 1, fast)

		// Now a query that deliberately sleeps past the configured timeout.
		// PostgreSQL should abort it server-side with SQLSTATE 57014
		// (query_canceled), surfaced by lib/pq with the canonical message
		// "pq: canceling statement due to statement timeout".
		_, err = rawDB.ExecContext(ctx, "SELECT pg_sleep(2)")
		assert.Error(t, err, "query exceeding statement_timeout should fail")
		assert.Contains(t, err.Error(), "statement timeout",
			"error should identify the server-side timeout source")
	})
}

func TestPostgreSQL_WriteMethodsFailCleanlyOnCancelledContext(t *testing.T) {
	t.Parallel()
	testWriteMethodsFailCleanlyOnCancelledContext(t, newTestPostgreSQLProvider)
}

func TestPostgreSQL_WriteMethodsNoOpOnEmptyInput(t *testing.T) {
	t.Parallel()
	testWriteMethodsNoOpOnEmptyInput(t, newTestPostgreSQLProvider)
}

// TestPostgreSQL_Insert_RollsBackOnConstraintViolation exercises Insert's
// exec-failure-mid-batch rollback path with a real, non-contrived failure:
// queries.statuscode is SMALLINT in PostgreSQL, so a value outside int16
// range is genuinely rejected by Postgres. This is Postgres-only -
// SQLite's queries.statusCode is a dynamically-typed INTEGER column with no
// range enforcement, so the same batch succeeds there (verified directly;
// not a difference this test can meaningfully assert on the SQLite side).
func TestPostgreSQL_Insert_RollsBackOnConstraintViolation(t *testing.T) {
	t.Parallel()
	p, cleanup := newTestPostgreSQLProvider(t)
	defer cleanup()

	now := time.Now().UTC()
	err := p.Insert(context.Background(), []Query{
		{TS: now, QueryParam: "would_succeed_alone", TimeParam: now, StatusCode: 200, Type: QueryTypeInstant, LabelMatchers: LabelMatchers{{"__name__": "would_succeed_alone"}}},
		{TS: now, QueryParam: "out_of_range", TimeParam: now, StatusCode: 999999, Type: QueryTypeInstant, LabelMatchers: LabelMatchers{{"__name__": "out_of_range"}}},
	})
	assert.Error(t, err, "a statusCode outside smallint range must fail the batch")
	assert.Contains(t, err.Error(), "out of range")

	p.WithDB(func(rawDB *sql.DB) {
		var count int
		rawErr := rawDB.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM queries").Scan(&count)
		assert.NoError(t, rawErr)
		assert.Equal(t, 0, count, "the whole batch must roll back, including the row that would have succeeded alone")
	})
}
