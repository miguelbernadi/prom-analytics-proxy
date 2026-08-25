package db

// Shared, provider-parameterized test suite.
//
// PostgreSQLProvider and SQLiteProvider both implement Provider and are
// expected to behave identically for the same inputs. This file holds those
// shared assertions once, taking a provider constructor so each backend's
// thin TestPostgreSQL_X / TestSQLite_X wrapper can exercise them. See
// https://github.com/nicolastakashi/prom-analytics-proxy/issues/577.
//
// Genuinely backend-specific tests are NOT here - they stay in their own
// file. That's either because the behavior itself is backend-only
// (TestPostgreSQL_StatementTimeoutAborts, TestNewPostgreSQLProvider_DoesNotMutateGlobalConfig),
// or because the test needs to poke the raw *sql.DB with dialect-specific SQL
// to set up a state the Provider interface can't otherwise reach - via
// p.WithDB, never a type assertion to the concrete provider struct - such as
// TestXProvider_DeleteQueriesBefore, TestX_TimeRangeDistribution_ISO_TZ,
// TestX_DashboardUsage's first_seen_at/last_seen_at seeding, and the
// presence/freshness-window backdating behind the RefreshMetricsUsageSummary
// exclusion tests. Forcing those into this shared shape would mean
// templating SQL text itself across dialects, which trades explicit,
// readable per-backend SQL for an abstraction - not a trade this package
// makes elsewhere (see the design note on execUpsertMany in common.go), so
// those stay as pairs of independent, explicit test functions instead.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/nicolastakashi/prom-analytics-proxy/api/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type newProviderFunc func(*testing.T) (Provider, func())

// -------------------- Shared seed helpers --------------------

func mustInsertQueries(t *testing.T, p Provider, qs []Query) {
	t.Helper()
	err := p.Insert(context.Background(), qs)
	require.NoError(t, err, "Insert")
}

func mustUpsertCatalog(t *testing.T, p Provider, items []MetricCatalogItem) {
	t.Helper()
	err := p.UpsertMetricsCatalog(context.Background(), items)
	require.NoError(t, err, "UpsertMetricsCatalog")
}

func mustUpsertJobIndex(t *testing.T, p Provider, items []MetricJobIndexItem) {
	t.Helper()
	err := p.UpsertMetricsJobIndex(context.Background(), items)
	require.NoError(t, err, "UpsertMetricsJobIndex")
}

func mustInsertRules(t *testing.T, p Provider, items []RulesUsage) {
	t.Helper()
	err := p.InsertRulesUsage(context.Background(), items)
	require.NoError(t, err, "InsertRulesUsage")
}

func mustInsertDashboards(t *testing.T, p Provider, items []DashboardUsage) {
	t.Helper()
	err := p.InsertDashboardUsage(context.Background(), items)
	require.NoError(t, err, "InsertDashboardUsage")
}

// summaryRow mirrors the four counts + is_unused columns of
// metrics_usage_summary, so a stale-vs-fresh diagnostic assertion can compare
// (and print, on failure) the whole row at once instead of one column at a
// time - useful since is_unused is derived from all four counts together.
// Read back via each backend's own mustSummaryRow helper (postgresql_test.go,
// sqlite_test.go): both need dialect-specific SQL to reach the presence/
// freshness columns under test, so they stay backend-specific per this
// file's own convention, even though the row shape they return is shared.
type summaryRow struct {
	Alert, Record, Dashboard, Query int
	Unused                          bool
}

// -------------------- Analytics --------------------

func testGetQueryTypes(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Minute)
	qs := make([]Query, 0, 10)
	for i := range 4 { // 4 instant
		qs = append(qs, Query{
			TS:            now.Add(time.Duration(i) * time.Minute),
			QueryParam:    "up",
			TimeParam:     now,
			Duration:      10 * time.Millisecond,
			StatusCode:    200,
			BodySize:      1,
			LabelMatchers: LabelMatchers{{"__name__": "up"}},
			Type:          QueryTypeInstant,
			Fingerprint:   "fp1",
		})
	}
	for i := range 6 { // 6 range
		qs = append(qs, Query{
			TS:            now.Add(time.Duration(i) * time.Minute),
			QueryParam:    "rate(up[5m])",
			TimeParam:     now,
			Duration:      15 * time.Millisecond,
			StatusCode:    200,
			BodySize:      1,
			LabelMatchers: LabelMatchers{{"__name__": "up"}},
			Type:          QueryTypeRange,
			Start:         now.Add(-5 * time.Minute),
			End:           now,
			Step:          15,
			Fingerprint:   "fp2",
		})
	}
	mustInsertQueries(t, p, qs)

	tr := TimeRange{From: now.Add(-10 * time.Minute), To: now.Add(10 * time.Minute)}
	out, err := p.GetQueryTypes(context.Background(), tr, "")
	assert.NoError(t, err, "GetQueryTypes")
	if assert.NotNil(t, out) {
		assert.NotNil(t, out.TotalQueries)
		assert.NotNil(t, out.InstantPercent)
		assert.NotNil(t, out.RangePercent)
		assert.Equal(t, 10, *out.TotalQueries)
		// 4/10 = 40, 6/10 = 60
		assert.InDelta(t, 40.0, *out.InstantPercent, 0.5)
		assert.InDelta(t, 60.0, *out.RangePercent, 0.5)
	}
}

func testGetAverageDuration(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	base := time.Date(2025, 8, 20, 12, 0, 0, 0, time.UTC)
	// previous window: [base-20m, base-10m)
	prevFrom := base.Add(-20 * time.Minute)
	// current window: [base-10m, base)
	curFrom := base.Add(-10 * time.Minute)
	curTo := base

	// Seed previous with avg 10ms, current with avg 20ms
	var qs []Query
	for i := range 5 {
		qs = append(qs, Query{
			TS:            prevFrom.Add(time.Duration(i) * time.Minute),
			QueryParam:    "up",
			TimeParam:     prevFrom,
			Duration:      10 * time.Millisecond,
			StatusCode:    200,
			LabelMatchers: LabelMatchers{{"__name__": "up"}},
			Type:          QueryTypeInstant,
		})
	}
	for i := range 5 {
		qs = append(qs, Query{
			TS:            curFrom.Add(time.Duration(i) * time.Minute),
			QueryParam:    "up",
			TimeParam:     curFrom,
			Duration:      20 * time.Millisecond,
			StatusCode:    200,
			LabelMatchers: LabelMatchers{{"__name__": "up"}},
			Type:          QueryTypeInstant,
		})
	}
	mustInsertQueries(t, p, qs)

	out, err := p.GetAverageDuration(context.Background(), TimeRange{From: curFrom, To: curTo}, "")
	assert.NoError(t, err, "GetAverageDuration")
	if assert.NotNil(t, out) {
		assert.NotNil(t, out.AvgDuration)
		assert.NotNil(t, out.DeltaPercent)
		assert.InDelta(t, 20.0, *out.AvgDuration, 0.5)
		// delta = ((20-10)/10)*100 = 100%; wide tolerance to absorb
		// backend-specific rounding/float differences in the underlying calc.
		assert.InDelta(t, 100.0, *out.DeltaPercent, 30.0)
	}
}

func testGetQueryRate(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Minute)
	qs := make([]Query, 0, 5)
	// 3 successes, 2 errors for metric "up" and fingerprint fp1
	for i := range 3 {
		qs = append(qs, Query{
			TS:            now.Add(time.Duration(i) * time.Minute),
			QueryParam:    "up",
			TimeParam:     now,
			Duration:      5 * time.Millisecond,
			StatusCode:    200,
			LabelMatchers: LabelMatchers{{"__name__": "up"}},
			Type:          QueryTypeInstant,
			Fingerprint:   "fp1",
		})
	}
	for i := range 2 {
		qs = append(qs, Query{
			TS:            now.Add(time.Duration(3+i) * time.Minute),
			QueryParam:    "up",
			TimeParam:     now,
			Duration:      5 * time.Millisecond,
			StatusCode:    500,
			LabelMatchers: LabelMatchers{{"__name__": "up"}},
			Type:          QueryTypeInstant,
			Fingerprint:   "fp1",
		})
	}
	mustInsertQueries(t, p, qs)

	tr := TimeRange{From: now.Add(-5 * time.Minute), To: now.Add(10 * time.Minute)}
	out, err := p.GetQueryRate(context.Background(), tr, "up", "fp1")
	assert.NoError(t, err, "GetQueryRate")
	if assert.NotNil(t, out) {
		assert.NotNil(t, out.SuccessTotal)
		assert.NotNil(t, out.ErrorTotal)
		assert.NotNil(t, out.SuccessRatePercent)
		assert.NotNil(t, out.ErrorRatePercent)
		assert.Equal(t, 3, *out.SuccessTotal)
		assert.Equal(t, 2, *out.ErrorTotal)
		assert.InDelta(t, 60.0, *out.SuccessRatePercent, 0.5)
		assert.InDelta(t, 40.0, *out.ErrorRatePercent, 0.5)
	}
}

func testGetQueryLatencyTrendsAndThroughputAndErrors(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Minute)
	qs := make([]Query, 0, 13)
	for i := range 10 {
		qs = append(qs, Query{
			TS:            now.Add(time.Duration(i) * time.Minute),
			QueryParam:    "up",
			TimeParam:     now,
			Duration:      time.Duration(5+i) * time.Millisecond,
			StatusCode:    200,
			BodySize:      1,
			LabelMatchers: LabelMatchers{{"__name__": "up"}},
			Type:          QueryTypeInstant,
			Fingerprint:   "fp-lat",
		})
	}
	// Add some errors spread across time
	for i := range 3 {
		qs = append(qs, Query{
			TS:            now.Add(time.Duration(i*2) * time.Minute),
			QueryParam:    "up",
			TimeParam:     now,
			Duration:      10 * time.Millisecond,
			StatusCode:    500,
			LabelMatchers: LabelMatchers{{"__name__": "up"}},
			Type:          QueryTypeInstant,
			Fingerprint:   "fp-lat",
		})
	}
	mustInsertQueries(t, p, qs)

	tr := TimeRange{From: now.Add(-10 * time.Minute), To: now.Add(20 * time.Minute)}

	lat, err := p.GetQueryLatencyTrends(context.Background(), tr, "up", "fp-lat")
	assert.NoError(t, err, "GetQueryLatencyTrends")
	assert.NotEmpty(t, lat)

	thr, err := p.GetQueryThroughputAnalysis(context.Background(), tr)
	assert.NoError(t, err, "GetQueryThroughputAnalysis")
	assert.NotEmpty(t, thr)

	errSeries, err := p.GetQueryErrorAnalysis(context.Background(), tr, "fp-lat")
	assert.NoError(t, err, "GetQueryErrorAnalysis")
	assert.NotEmpty(t, errSeries)

	// Also validate status distribution for completeness
	dist, err := p.GetQueryStatusDistribution(context.Background(), tr, "fp-lat")
	assert.NoError(t, err, "GetQueryStatusDistribution")
	assert.NotEmpty(t, dist)
}

func testGetQueriesBySerieName(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	// Seed a mix of queries for two queryParam values of the same metric
	now := time.Now().UTC()
	qs := []Query{
		{TS: now.Add(-2 * time.Minute), QueryParam: "up", TimeParam: now, Duration: 10 * time.Millisecond, StatusCode: 200, LabelMatchers: LabelMatchers{{"__name__": "up"}}, Type: QueryTypeInstant, PeakSamples: 100},
		{TS: now.Add(-1 * time.Minute), QueryParam: "up", TimeParam: now, Duration: 20 * time.Millisecond, StatusCode: 200, LabelMatchers: LabelMatchers{{"__name__": "up"}}, Type: QueryTypeInstant, PeakSamples: 200},
		{TS: now.Add(-2 * time.Minute), QueryParam: "rate(up[5m])", TimeParam: now, Duration: 30 * time.Millisecond, StatusCode: 200, LabelMatchers: LabelMatchers{{"__name__": "up"}}, Type: QueryTypeRange, Start: now.Add(-5 * time.Minute), End: now, PeakSamples: 300},
	}
	mustInsertQueries(t, p, qs)

	res, err := p.GetQueriesBySerieName(context.Background(), QueriesBySerieNameParams{
		SerieName: "up",
		TimeRange: TimeRange{From: now.Add(-1 * time.Hour), To: now.Add(1 * time.Hour)},
		Page:      1,
		PageSize:  10,
		SortBy:    "avgDuration",
		SortOrder: "desc",
	})
	assert.NoError(t, err, "GetQueriesBySerieName")
	assert.Greater(t, res.Total, 0)
	assert.IsType(t, []QueriesBySerieNameResult{}, res.Data)
	rows, _ := res.Data.([]QueriesBySerieNameResult)
	assert.Len(t, rows, 2)
}

func testGetQueryExpressionsAndExecutions(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Minute)
	qs := make([]Query, 0, 8)
	// Two fingerprints, one with more executions
	for i := range 5 {
		qs = append(qs, Query{
			TS:            now.Add(time.Duration(i) * time.Minute),
			QueryParam:    "up",
			TimeParam:     now,
			Duration:      10 * time.Millisecond,
			StatusCode:    200,
			LabelMatchers: LabelMatchers{{"__name__": "up"}},
			Type:          QueryTypeInstant,
			PeakSamples:   10 + i,
			Fingerprint:   "fp-a",
		})
	}
	for i := range 3 {
		qs = append(qs, Query{
			TS:            now.Add(time.Duration(i) * time.Minute),
			QueryParam:    "rate(up[5m])",
			TimeParam:     now,
			Duration:      20 * time.Millisecond,
			StatusCode:    500,
			LabelMatchers: LabelMatchers{{"__name__": "up"}},
			Type:          QueryTypeRange,
			Start:         now.Add(-5 * time.Minute),
			End:           now,
			Step:          15,
			PeakSamples:   100,
			Fingerprint:   "fp-b",
		})
	}
	mustInsertQueries(t, p, qs)

	// QueryExpressions
	pr, err := p.GetQueryExpressions(context.Background(), QueryExpressionsParams{
		TimeRange: TimeRange{From: now.Add(-1 * time.Hour), To: now.Add(1 * time.Hour)},
		Page:      1,
		PageSize:  10,
		SortBy:    "executions",
		SortOrder: "desc",
	})
	assert.NoError(t, err, "GetQueryExpressions")
	rows2, ok := pr.Data.([]QueryExpression)
	if assert.True(t, ok, "type conversion") {
		assert.Len(t, rows2, 2)
		assert.Equal(t, "fp-a", rows2[0].Fingerprint)
	}

	// QueryExecutions for fp-b, type range (table-driven for sort order)
	cases := []struct{ sortOrder string }{{"asc"}, {"desc"}}
	for _, tc := range cases {
		per, err := p.GetQueryExecutions(context.Background(), QueryExecutionsParams{
			Fingerprint: "fp-b",
			Type:        string(QueryTypeRange),
			TimeRange:   TimeRange{From: now.Add(-1 * time.Hour), To: now.Add(1 * time.Hour)},
			Page:        1,
			PageSize:    10,
			SortBy:      "ts",
			SortOrder:   tc.sortOrder,
		})
		assert.NoError(t, err, "GetQueryExecutions")
		erows, ok := per.Data.([]QueryExecutionRow)
		if assert.True(t, ok, "type conversion") {
			assert.Len(t, erows, 3)
			if tc.sortOrder == "asc" {
				assert.True(t, erows[0].Timestamp.Before(erows[2].Timestamp) || erows[0].Timestamp.Equal(erows[2].Timestamp))
			} else {
				assert.True(t, erows[0].Timestamp.After(erows[2].Timestamp) || erows[0].Timestamp.Equal(erows[2].Timestamp))
			}
		}
	}
}

func testGetMetricStatisticsAndQueryPerformanceStats(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	metric := "http_requests_total"
	now := time.Now().UTC().Truncate(time.Minute)

	// Seed rules and dashboards across different series. InsertRulesUsage /
	// InsertDashboardUsage set first_seen_at and last_seen_at to current time.
	mustInsertRules(t, p, []RulesUsage{
		{Serie: metric, GroupName: "g", Name: "a1", Expression: "expr", Kind: string(RuleUsageKindAlert), Labels: []string{"l"}, CreatedAt: now.Add(-30 * time.Minute)},
		{Serie: metric, GroupName: "g", Name: "r1", Expression: "expr", Kind: string(RuleUsageKindRecord), Labels: []string{"l"}, CreatedAt: now.Add(-30 * time.Minute)},
		{Serie: "other", GroupName: "g", Name: "a2", Expression: "expr", Kind: string(RuleUsageKindAlert), Labels: []string{"l"}, CreatedAt: now.Add(-30 * time.Minute)},
	})
	mustInsertDashboards(t, p, []DashboardUsage{
		{Id: "d1", Serie: metric, Name: "Dash", URL: "http://d/1", CreatedAt: now.Add(-45 * time.Minute)},
		{Id: "d2", Serie: "other", Name: "Other", URL: "http://d/2", CreatedAt: now.Add(-45 * time.Minute)},
	})

	// Seed queries for performance stats
	mustInsertQueries(t, p, []Query{
		{TS: now.Add(-5 * time.Minute), QueryParam: metric, TimeParam: now, Duration: 12 * time.Millisecond, StatusCode: 200, LabelMatchers: LabelMatchers{{"__name__": metric}}, Type: QueryTypeInstant, PeakSamples: 100},
		{TS: now.Add(-4 * time.Minute), QueryParam: metric, TimeParam: now, Duration: 18 * time.Millisecond, StatusCode: 200, LabelMatchers: LabelMatchers{{"__name__": metric}}, Type: QueryTypeInstant, PeakSamples: 200},
	})

	// Time range that includes "now" (when first_seen_at/last_seen_at were set)
	tr := TimeRange{From: now.Add(-1 * time.Hour), To: now.Add(1 * time.Hour)}

	stats, err := p.GetMetricStatistics(context.Background(), metric, tr)
	assert.NoError(t, err, "GetMetricStatistics")
	assert.Greater(t, stats.AlertCount, 0)
	assert.Greater(t, stats.RecordCount, 0)
	assert.Greater(t, stats.DashboardCount, 0)

	perf, err := p.GetMetricQueryPerformanceStatistics(context.Background(), metric, tr)
	assert.NoError(t, err, "GetMetricQueryPerformanceStatistics")
	if assert.NotNil(t, perf.TotalQueries) {
		assert.Greater(t, *perf.TotalQueries, 0)
	}
}

func testQueryTimeRangeDistribution(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	now := time.Now().UTC()

	// Helper to create a range query with the given window duration
	mkRange := func(window time.Duration) Query {
		return Query{
			TS:            now.Add(-5 * time.Minute),
			QueryParam:    "up",
			TimeParam:     now.Add(-5 * time.Minute),
			Duration:      5 * time.Millisecond,
			StatusCode:    200,
			BodySize:      1,
			LabelMatchers: LabelMatchers{{"__name__": "up"}},
			Type:          QueryTypeRange,
			Step:          15,
			Start:         now.Add(-5 * time.Minute).Add(-window),
			End:           now.Add(-5 * time.Minute),
		}
	}

	// Seed queries across buckets
	var qs []Query
	for range 5 { // <24h
		qs = append(qs, mkRange(5*time.Minute))
	}
	for range 3 { // 24h–<7d
		qs = append(qs, mkRange(48*time.Hour))
	}
	for range 2 { // 7d–<30d
		qs = append(qs, mkRange(8*24*time.Hour))
	}
	qs = append(qs, mkRange(31*24*time.Hour))  // 30d–<60d
	qs = append(qs, mkRange(65*24*time.Hour))  // 60d–<90d
	qs = append(qs, mkRange(100*24*time.Hour)) // 90d+

	// Insert a few instant queries that must be ignored
	qs = append(qs, Query{
		TS:            now.Add(-2 * time.Minute),
		QueryParam:    "up",
		TimeParam:     now.Add(-2 * time.Minute),
		Duration:      3 * time.Millisecond,
		StatusCode:    200,
		BodySize:      1,
		LabelMatchers: LabelMatchers{{"__name__": "up"}},
		Type:          QueryTypeInstant,
	})

	err := p.Insert(context.Background(), qs)
	assert.NoError(t, err, "Insert")

	// Query distribution within a window that includes TS
	out, err := p.GetQueryTimeRangeDistribution(context.Background(), TimeRange{From: now.Add(-24 * time.Hour), To: now}, "")
	assert.NoError(t, err, "GetQueryTimeRangeDistribution")

	// Build result map for easy assertions
	got := map[string]int{}
	total := 0
	for _, b := range out {
		got[b.Label] = b.Count
		total += b.Count
	}

	// Expect 5+3+2+1+1+1 = 13
	assert.Equal(t, 13, total, "unexpected total count")
	assert.Equal(t, 5, got["<24h"], "unexpected bucket count for <24h")
	assert.Equal(t, 3, got["24h"], "unexpected bucket count for 24h")
	assert.Equal(t, 2, got["7d"], "unexpected bucket count for 7d")
	assert.Equal(t, 1, got["30d"], "unexpected bucket count for 30d")
	assert.Equal(t, 1, got["60d"], "unexpected bucket count for 60d")
	assert.Equal(t, 1, got["90d+"], "unexpected bucket count for 90d+")
}

// testAnalyticsMethodsOnEmptyDatabase verifies GetQueryTypes,
// GetAverageDuration, and GetQueryRate return without error against an
// empty queries table - the state a freshly-deployed proxy starts in -
// rather than erroring on the divide-by-zero their SQL guards against. A
// count field always reads back as 0 when non-nil; the two backends differ
// on whether a zero-input percentage/average comes back as nil or 0
// (NULLIF vs. SUM-over-no-rows are both legitimately NULL, while a plain
// COUNT is always 0), so this only asserts the non-nil case is exactly 0.
func testAnalyticsMethodsOnEmptyDatabase(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	now := time.Now().UTC()
	tr := TimeRange{From: now.Add(-time.Hour), To: now}

	t.Run("GetQueryTypes", func(t *testing.T) {
		out, err := p.GetQueryTypes(context.Background(), tr, "")
		assert.NoError(t, err)
		if assert.NotNil(t, out) && assert.NotNil(t, out.TotalQueries) {
			assert.Zero(t, *out.TotalQueries)
		}
		if out != nil && out.InstantPercent != nil {
			assert.Zero(t, *out.InstantPercent)
		}
		if out != nil && out.RangePercent != nil {
			assert.Zero(t, *out.RangePercent)
		}
	})

	t.Run("GetAverageDuration", func(t *testing.T) {
		out, err := p.GetAverageDuration(context.Background(), tr, "")
		assert.NoError(t, err)
		if assert.NotNil(t, out) {
			if out.AvgDuration != nil {
				assert.Zero(t, *out.AvgDuration)
			}
			if out.DeltaPercent != nil {
				assert.Zero(t, *out.DeltaPercent)
			}
		}
	})

	t.Run("GetQueryRate", func(t *testing.T) {
		out, err := p.GetQueryRate(context.Background(), tr, "up", "")
		assert.NoError(t, err)
		if assert.NotNil(t, out) {
			if out.SuccessTotal != nil {
				assert.Zero(t, *out.SuccessTotal)
			}
			if out.ErrorTotal != nil {
				assert.Zero(t, *out.ErrorTotal)
			}
		}
	})
}

// testGetQueriesBySerieNamePagination verifies GetQueriesBySerieName's paged
// result advances across pages without duplicating or dropping rows, and
// that a page past the last one returns an empty result rather than an
// error.
func testGetQueriesBySerieNamePagination(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	now := time.Now().UTC()
	var qs []Query
	for i := 1; i <= 5; i++ {
		qs = append(qs, Query{
			TS:            now.Add(-time.Duration(i) * time.Minute),
			QueryParam:    fmt.Sprintf("up_variant_%d", i),
			TimeParam:     now,
			Duration:      time.Duration(i*10) * time.Millisecond,
			StatusCode:    200,
			LabelMatchers: LabelMatchers{{"__name__": "up"}},
			Type:          QueryTypeInstant,
		})
	}
	mustInsertQueries(t, p, qs)

	baseParams := QueriesBySerieNameParams{
		SerieName: "up",
		TimeRange: TimeRange{From: now.Add(-1 * time.Hour), To: now.Add(1 * time.Hour)},
		PageSize:  2,
		SortBy:    "avgDuration",
		SortOrder: "desc",
	}

	seen := map[string]bool{}
	for page := 1; page <= 3; page++ {
		params := baseParams
		params.Page = page
		out, err := p.GetQueriesBySerieName(context.Background(), params)
		assert.NoError(t, err, "page %d", page)
		assert.Equal(t, 5, out.Total)
		rows, ok := out.Data.([]QueriesBySerieNameResult)
		if !assert.True(t, ok, "type conversion") {
			continue
		}
		wantLen := 2
		if page == 3 {
			wantLen = 1
		}
		assert.Len(t, rows, wantLen, "page %d", page)
		for _, r := range rows {
			assert.False(t, seen[r.Query], "query %q returned on more than one page", r.Query)
			seen[r.Query] = true
		}
	}
	assert.Len(t, seen, 5, "all 5 rows must be reachable across pages")

	params := baseParams
	params.Page = 4
	out, err := p.GetQueriesBySerieName(context.Background(), params)
	assert.NoError(t, err, "page past last")
	assert.Equal(t, 5, out.Total)
	if rows, ok := out.Data.([]QueriesBySerieNameResult); assert.True(t, ok, "type conversion") {
		assert.Empty(t, rows)
	}
}

// testGetQueryExpressionsPagination verifies GetQueryExpressions' paged
// result advances across pages without duplicating or dropping rows.
func testGetQueryExpressionsPagination(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Minute)
	var qs []Query
	for fp := 1; fp <= 5; fp++ {
		for i := 0; i < fp; i++ { // fp1 gets 1 execution, ..., fp5 gets 5
			qs = append(qs, Query{
				TS:            now.Add(time.Duration(i) * time.Minute),
				QueryParam:    fmt.Sprintf("expr_%d", fp),
				TimeParam:     now,
				Duration:      10 * time.Millisecond,
				StatusCode:    200,
				LabelMatchers: LabelMatchers{{"__name__": "up"}},
				Type:          QueryTypeInstant,
				Fingerprint:   fmt.Sprintf("fp%d", fp),
			})
		}
	}
	mustInsertQueries(t, p, qs)

	baseParams := QueryExpressionsParams{
		TimeRange: TimeRange{From: now.Add(-1 * time.Hour), To: now.Add(1 * time.Hour)},
		PageSize:  2,
		SortBy:    "executions",
		SortOrder: "desc",
	}

	seen := map[string]bool{}
	for page := 1; page <= 3; page++ {
		params := baseParams
		params.Page = page
		out, err := p.GetQueryExpressions(context.Background(), params)
		assert.NoError(t, err, "page %d", page)
		assert.Equal(t, 5, out.Total)
		rows, ok := out.Data.([]QueryExpression)
		if !assert.True(t, ok, "type conversion") {
			continue
		}
		wantLen := 2
		if page == 3 {
			wantLen = 1
		}
		assert.Len(t, rows, wantLen, "page %d", page)
		for _, r := range rows {
			assert.False(t, seen[r.Fingerprint], "fingerprint %q returned on more than one page", r.Fingerprint)
			seen[r.Fingerprint] = true
		}
	}
	assert.Len(t, seen, 5, "all 5 rows must be reachable across pages")

	params := baseParams
	params.Page = 4
	out, err := p.GetQueryExpressions(context.Background(), params)
	assert.NoError(t, err, "page past last")
	assert.Equal(t, 5, out.Total)
	if rows, ok := out.Data.([]QueryExpression); assert.True(t, ok, "type conversion") {
		assert.Empty(t, rows)
	}
}

// testGetQueryExecutionsPaginationTypeFilterAndHTTPHeaders verifies
// GetQueryExecutions across three real caller variations: paging through
// results without gaps or duplicates, filtering by Type ("" matches every
// type, "instant" and "range" narrow to just one), and round-tripping a
// captured HTTPHeaders map through Insert and back out.
func testGetQueryExecutionsPaginationTypeFilterAndHTTPHeaders(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Minute)
	headers := map[string]string{"X-Request-Id": "abc123"}
	qs := []Query{
		{TS: now, QueryParam: "up", TimeParam: now, Duration: 10 * time.Millisecond, StatusCode: 200, LabelMatchers: LabelMatchers{{"__name__": "up"}}, Type: QueryTypeInstant, Fingerprint: "fp-mixed", HTTPHeaders: headers},
		{TS: now.Add(time.Minute), QueryParam: "up", TimeParam: now, Duration: 10 * time.Millisecond, StatusCode: 200, LabelMatchers: LabelMatchers{{"__name__": "up"}}, Type: QueryTypeInstant, Fingerprint: "fp-mixed"},
		{TS: now.Add(2 * time.Minute), QueryParam: "up", TimeParam: now, Duration: 10 * time.Millisecond, StatusCode: 200, LabelMatchers: LabelMatchers{{"__name__": "up"}}, Type: QueryTypeInstant, Fingerprint: "fp-mixed"},
		{TS: now.Add(3 * time.Minute), QueryParam: "rate(up[5m])", TimeParam: now, Duration: 20 * time.Millisecond, StatusCode: 200, LabelMatchers: LabelMatchers{{"__name__": "up"}}, Type: QueryTypeRange, Start: now.Add(-5 * time.Minute), End: now, Step: 15, Fingerprint: "fp-mixed"},
		{TS: now.Add(4 * time.Minute), QueryParam: "rate(up[5m])", TimeParam: now, Duration: 20 * time.Millisecond, StatusCode: 200, LabelMatchers: LabelMatchers{{"__name__": "up"}}, Type: QueryTypeRange, Start: now.Add(-5 * time.Minute), End: now, Step: 15, Fingerprint: "fp-mixed"},
	}
	mustInsertQueries(t, p, qs)

	tr := TimeRange{From: now.Add(-1 * time.Hour), To: now.Add(1 * time.Hour)}

	t.Run("pagination", func(t *testing.T) {
		baseParams := QueryExecutionsParams{Fingerprint: "fp-mixed", TimeRange: tr, PageSize: 2, SortBy: "ts", SortOrder: "asc"}

		seen := map[time.Time]bool{}
		for page := 1; page <= 3; page++ {
			params := baseParams
			params.Page = page
			out, err := p.GetQueryExecutions(context.Background(), params)
			assert.NoError(t, err, "page %d", page)
			assert.Equal(t, 5, out.Total)
			rows, ok := out.Data.([]QueryExecutionRow)
			if !assert.True(t, ok, "type conversion") {
				continue
			}
			wantLen := 2
			if page == 3 {
				wantLen = 1
			}
			assert.Len(t, rows, wantLen, "page %d", page)
			for _, r := range rows {
				assert.False(t, seen[r.Timestamp], "timestamp %v returned on more than one page", r.Timestamp)
				seen[r.Timestamp] = true
			}
		}
		assert.Len(t, seen, 5, "all 5 rows must be reachable across pages")

		params := baseParams
		params.Page = 4
		out, err := p.GetQueryExecutions(context.Background(), params)
		assert.NoError(t, err, "page past last")
		assert.Equal(t, 5, out.Total)
		if rows, ok := out.Data.([]QueryExecutionRow); assert.True(t, ok, "type conversion") {
			assert.Empty(t, rows)
		}
	})

	t.Run("type filter", func(t *testing.T) {
		cases := []struct {
			name      string
			typeParam string
			wantLen   int
			wantType  string
		}{
			{name: "all (blank)", typeParam: "", wantLen: 5},
			{name: "instant", typeParam: string(QueryTypeInstant), wantLen: 3, wantType: string(QueryTypeInstant)},
			{name: "range", typeParam: string(QueryTypeRange), wantLen: 2, wantType: string(QueryTypeRange)},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				out, err := p.GetQueryExecutions(context.Background(), QueryExecutionsParams{
					Fingerprint: "fp-mixed", TimeRange: tr, Page: 1, PageSize: 10, SortBy: "ts", SortOrder: "asc", Type: tc.typeParam,
				})
				assert.NoError(t, err)
				rows, ok := out.Data.([]QueryExecutionRow)
				if !assert.True(t, ok, "type conversion") {
					return
				}
				assert.Len(t, rows, tc.wantLen)
				if tc.wantType != "" {
					for _, r := range rows {
						assert.Equal(t, tc.wantType, r.Type)
					}
				}
			})
		}
	})

	t.Run("HTTPHeaders round-trip", func(t *testing.T) {
		out, err := p.GetQueryExecutions(context.Background(), QueryExecutionsParams{
			Fingerprint: "fp-mixed", TimeRange: tr, Page: 1, PageSize: 10, SortBy: "ts", SortOrder: "asc", Type: string(QueryTypeInstant),
		})
		assert.NoError(t, err)
		rows, ok := out.Data.([]QueryExecutionRow)
		if !assert.True(t, ok, "type conversion") || !assert.NotEmpty(t, rows) {
			return
		}
		assert.Equal(t, headers, rows[0].HTTPHeaders, "the first inserted row's HTTPHeaders must round-trip through Insert and back")
	})
}

// -------------------- Rules / Dashboard usage --------------------

func testInsertRulesUsageGetRulesUsage(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	base := time.Date(2025, 8, 18, 20, 0, 0, 0, time.UTC)
	rules := []RulesUsage{
		{Serie: "up", GroupName: "g1", Name: "r1", Expression: "expr1", Kind: string(RuleUsageKindAlert), Labels: []string{"l2", "l1"}, CreatedAt: base},
		// duplicate (different label order) should de-duplicate
		{Serie: "up", GroupName: "g1", Name: "r1", Expression: "expr1", Kind: string(RuleUsageKindAlert), Labels: []string{"l1", "l2"}, CreatedAt: base},
		// different rule
		{Serie: "up", GroupName: "g2", Name: "r2", Expression: "expr2", Kind: string(RuleUsageKindRecord), Labels: []string{"lbl"}, CreatedAt: base},
	}
	mustInsertRules(t, p, rules)

	// Use a time range that includes the current time (when first_seen_at and last_seen_at are set)
	now := time.Now().UTC()
	out, err := p.GetRulesUsage(context.Background(), RulesUsageParams{
		Serie:     "up",
		Kind:      string(RuleUsageKindAlert),
		TimeRange: TimeRange{From: now.Add(-1 * time.Hour), To: now.Add(1 * time.Hour)},
		Page:      1,
		PageSize:  10,
	})
	assert.NoError(t, err, "GetRulesUsage")
	rows, ok := out.Data.([]RulesUsage)
	if assert.True(t, ok, "type conversion") {
		// Only the alert kind for serie=up -> expect 1 unique after dedup
		assert.Len(t, rows, 1)
	}
}

func testInsertDashboardUsageUpsertBehavior(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	base := time.Now().UTC().Truncate(time.Minute)
	mustInsertDashboards(t, p, []DashboardUsage{{Id: "d1", Serie: "m1", Name: "Dash 1", URL: "http://d/1", CreatedAt: base}})
	// Upsert with newer last_seen should update name/url and extend presence
	mustInsertDashboards(t, p, []DashboardUsage{{Id: "d1", Serie: "m1", Name: "Dash 1 Renamed", URL: "http://d/1r", CreatedAt: base}})

	out, err := p.GetDashboardUsage(context.Background(), DashboardUsageParams{
		Serie:     "m1",
		TimeRange: TimeRange{From: base.Add(-1 * time.Hour), To: base.Add(1 * time.Hour)},
		Page:      1,
		PageSize:  10,
	})
	assert.NoError(t, err, "GetDashboardUsage")
	rows, ok := out.Data.([]DashboardUsage)
	if assert.True(t, ok, "type conversion") {
		assert.Len(t, rows, 1)
		assert.Equal(t, "Dash 1 Renamed", rows[0].Name)
		assert.Equal(t, "http://d/1r", rows[0].URL)
	}
}

func ruleSortViolation(a, b RulesUsage, sortBy string) bool {
	switch sortBy {
	case "name":
		return a.Name > b.Name
	case "group_name":
		return a.GroupName > b.GroupName
	case "expression":
		return a.Expression > b.Expression
	case "created_at":
		return a.CreatedAt.After(b.CreatedAt)
	default:
		return false
	}
}

// testGetRulesUsageSortFieldsAndPagination verifies GetRulesUsage orders by
// whichever of its four whitelisted fields the caller asks for, and that
// its paged result advances across pages without duplicating or dropping
// rows.
func testGetRulesUsageSortFieldsAndPagination(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	base := time.Date(2025, 8, 18, 20, 0, 0, 0, time.UTC)
	mustInsertRules(t, p, []RulesUsage{
		{Serie: "sortsvc", GroupName: "gc", Name: "rb", Expression: "ec", Kind: string(RuleUsageKindAlert), Labels: []string{"l"}, CreatedAt: base.Add(4 * time.Minute)},
		{Serie: "sortsvc", GroupName: "ga", Name: "re", Expression: "ea", Kind: string(RuleUsageKindAlert), Labels: []string{"l"}, CreatedAt: base.Add(1 * time.Minute)},
		{Serie: "sortsvc", GroupName: "ge", Name: "rc", Expression: "ee", Kind: string(RuleUsageKindAlert), Labels: []string{"l"}, CreatedAt: base.Add(3 * time.Minute)},
		{Serie: "sortsvc", GroupName: "gb", Name: "ra", Expression: "eb", Kind: string(RuleUsageKindAlert), Labels: []string{"l"}, CreatedAt: base},
		{Serie: "sortsvc", GroupName: "gd", Name: "rd", Expression: "ed", Kind: string(RuleUsageKindAlert), Labels: []string{"l"}, CreatedAt: base.Add(2 * time.Minute)},
	})

	now := time.Now().UTC()
	tr := TimeRange{From: now.Add(-1 * time.Hour), To: now.Add(1 * time.Hour)}

	t.Run("sort fields", func(t *testing.T) {
		for _, sortBy := range []string{"name", "group_name", "expression", "created_at"} {
			t.Run(sortBy, func(t *testing.T) {
				out, err := p.GetRulesUsage(context.Background(), RulesUsageParams{
					Serie: "sortsvc", Kind: string(RuleUsageKindAlert), TimeRange: tr,
					Page: 1, PageSize: 10, SortBy: sortBy, SortOrder: "asc",
				})
				assert.NoError(t, err)
				rows, ok := out.Data.([]RulesUsage)
				if !assert.True(t, ok, "type conversion") || !assert.Len(t, rows, 5) {
					return
				}
				for i := 1; i < len(rows); i++ {
					assert.False(t, ruleSortViolation(rows[i-1], rows[i], sortBy),
						"row %d must not sort after row %d by %s", i-1, i, sortBy)
				}
			})
		}
	})

	t.Run("pagination", func(t *testing.T) {
		baseParams := RulesUsageParams{Serie: "sortsvc", Kind: string(RuleUsageKindAlert), TimeRange: tr, PageSize: 2, SortBy: "name", SortOrder: "asc"}
		seen := map[string]bool{}
		for page := 1; page <= 3; page++ {
			params := baseParams
			params.Page = page
			out, err := p.GetRulesUsage(context.Background(), params)
			assert.NoError(t, err, "page %d", page)
			assert.Equal(t, 5, out.Total)
			rows, ok := out.Data.([]RulesUsage)
			if !assert.True(t, ok, "type conversion") {
				continue
			}
			wantLen := 2
			if page == 3 {
				wantLen = 1
			}
			assert.Len(t, rows, wantLen, "page %d", page)
			for _, r := range rows {
				assert.False(t, seen[r.Name], "rule %q returned on more than one page", r.Name)
				seen[r.Name] = true
			}
		}
		assert.Len(t, seen, 5, "all 5 rows must be reachable across pages")

		params := baseParams
		params.Page = 4
		out, err := p.GetRulesUsage(context.Background(), params)
		assert.NoError(t, err, "page past last")
		if rows, ok := out.Data.([]RulesUsage); assert.True(t, ok, "type conversion") {
			assert.Empty(t, rows)
		}
	})
}

func dashboardSortViolation(a, b DashboardUsage, sortBy string) bool {
	switch sortBy {
	case "name":
		return a.Name > b.Name
	case "url":
		return a.URL > b.URL
	case "created_at":
		return a.CreatedAt.After(b.CreatedAt)
	default:
		return false
	}
}

// testGetDashboardUsageSortFieldsAndPagination verifies GetDashboardUsage
// orders by whichever of its three whitelisted fields the caller asks for,
// and that its paged result advances across pages without duplicating or
// dropping rows.
func testGetDashboardUsageSortFieldsAndPagination(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	base := time.Now().UTC().Truncate(time.Minute)
	mustInsertDashboards(t, p, []DashboardUsage{
		{Id: "d1", Serie: "sortsvc", Name: "Dash B", URL: "http://d/c", CreatedAt: base.Add(4 * time.Minute)},
		{Id: "d2", Serie: "sortsvc", Name: "Dash E", URL: "http://d/a", CreatedAt: base.Add(1 * time.Minute)},
		{Id: "d3", Serie: "sortsvc", Name: "Dash C", URL: "http://d/e", CreatedAt: base.Add(3 * time.Minute)},
		{Id: "d4", Serie: "sortsvc", Name: "Dash A", URL: "http://d/b", CreatedAt: base},
		{Id: "d5", Serie: "sortsvc", Name: "Dash D", URL: "http://d/d", CreatedAt: base.Add(2 * time.Minute)},
	})

	tr := TimeRange{From: base.Add(-1 * time.Hour), To: base.Add(1 * time.Hour)}

	t.Run("sort fields", func(t *testing.T) {
		for _, sortBy := range []string{"name", "url", "created_at"} {
			t.Run(sortBy, func(t *testing.T) {
				out, err := p.GetDashboardUsage(context.Background(), DashboardUsageParams{
					Serie: "sortsvc", TimeRange: tr, Page: 1, PageSize: 10, SortBy: sortBy, SortOrder: "asc",
				})
				assert.NoError(t, err)
				rows, ok := out.Data.([]DashboardUsage)
				if !assert.True(t, ok, "type conversion") || !assert.Len(t, rows, 5) {
					return
				}
				for i := 1; i < len(rows); i++ {
					assert.False(t, dashboardSortViolation(rows[i-1], rows[i], sortBy),
						"row %d must not sort after row %d by %s", i-1, i, sortBy)
				}
			})
		}
	})

	t.Run("pagination", func(t *testing.T) {
		baseParams := DashboardUsageParams{Serie: "sortsvc", TimeRange: tr, PageSize: 2, SortBy: "name", SortOrder: "asc"}
		seen := map[string]bool{}
		for page := 1; page <= 3; page++ {
			params := baseParams
			params.Page = page
			out, err := p.GetDashboardUsage(context.Background(), params)
			assert.NoError(t, err, "page %d", page)
			assert.Equal(t, 5, out.Total)
			rows, ok := out.Data.([]DashboardUsage)
			if !assert.True(t, ok, "type conversion") {
				continue
			}
			wantLen := 2
			if page == 3 {
				wantLen = 1
			}
			assert.Len(t, rows, wantLen, "page %d", page)
			for _, r := range rows {
				assert.False(t, seen[r.Id], "dashboard %q returned on more than one page", r.Id)
				seen[r.Id] = true
			}
		}
		assert.Len(t, seen, 5, "all 5 rows must be reachable across pages")

		params := baseParams
		params.Page = 4
		out, err := p.GetDashboardUsage(context.Background(), params)
		assert.NoError(t, err, "page past last")
		if rows, ok := out.Data.([]DashboardUsage); assert.True(t, ok, "type conversion") {
			assert.Empty(t, rows)
		}
	})
}

// -------------------- Metrics catalog / inventory / usage --------------------

func testMetricsJobIndexAndListJobs(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	mustUpsertJobIndex(t, p, []MetricJobIndexItem{
		{Name: "up", Job: "prometheus"},
		{Name: "up", Job: "node"},
		{Name: "process_cpu_seconds_total", Job: "node"},
	})

	jobs, err := p.ListJobs(context.Background())
	assert.NoError(t, err, "ListJobs")
	assert.ElementsMatch(t, []string{"node", "prometheus"}, jobs)
}

func testMetricsInventoryAndList(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	// Upsert catalog
	items := []MetricCatalogItem{{Name: "up", Type: "gauge", Help: "up metric", Unit: ""}}
	err := p.UpsertMetricsCatalog(context.Background(), items)
	assert.NoError(t, err, "UpsertMetricsCatalog")

	// Insert a few queries for window
	now := time.Now().UTC()
	err = p.Insert(context.Background(), []Query{{
		TS:            now.Add(-time.Hour),
		QueryParam:    "up",
		TimeParam:     now,
		Duration:      10 * time.Millisecond,
		StatusCode:    200,
		BodySize:      10,
		LabelMatchers: LabelMatchers{{"__name__": "up"}},
		Type:          QueryTypeInstant,
	}})
	assert.NoError(t, err, "Insert queries")

	// Refresh summary for 1 day window
	err = p.RefreshMetricsUsageSummary(context.Background(), TimeRange{From: now.Add(-24 * time.Hour), To: now})
	assert.NoError(t, err, "RefreshMetricsUsageSummary")

	// Read list
	res, err := p.GetSeriesMetadata(context.Background(), SeriesMetadataParams{Page: 1, PageSize: 10, SortBy: "name", SortOrder: "asc", Type: "all"})
	assert.NoError(t, err, "GetSeriesMetadata")
	assert.Greater(t, res.Total, 0, "expected at least one metric in catalog")
}

func testGetSeriesMetadataUsageFilters(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	now := time.Now().UTC()
	mustUpsertCatalog(t, p, []MetricCatalogItem{
		{Name: "used_metric", Type: "gauge", Help: "used metric"},
		{Name: "unused_metric", Type: "gauge", Help: "unused metric"},
	})
	mustInsertQueries(t, p, []Query{{
		TS:            now.Add(-5 * time.Minute),
		QueryParam:    "used_metric",
		TimeParam:     now,
		Duration:      10 * time.Millisecond,
		StatusCode:    200,
		LabelMatchers: LabelMatchers{{"__name__": "used_metric"}},
		Type:          QueryTypeInstant,
	}})

	assert.NoError(t, p.RefreshMetricsUsageSummary(context.Background(), TimeRange{From: now.Add(-1 * time.Hour), To: now}))
	// Catalogued after the only RefreshMetricsUsageSummary run: this metric has
	// never actually been evaluated, so it must not show up as "unused" - it
	// should behave as "unknown" until a future refresh evaluates it. See
	// https://github.com/nicolastakashi/prom-analytics-proxy/issues/570.
	mustUpsertCatalog(t, p, []MetricCatalogItem{{Name: "never_evaluated_metric", Type: "gauge", Help: "never evaluated metric"}})

	tests := []struct {
		name      string
		usage     string
		wantNames []string
	}{
		{name: "all metrics", usage: SeriesMetadataUsageAll, wantNames: []string{"never_evaluated_metric", "unused_metric", "used_metric"}},
		{name: "used metrics", usage: SeriesMetadataUsageUsed, wantNames: []string{"used_metric"}},
		{name: "unused metrics", usage: SeriesMetadataUsageUnused, wantNames: []string{"unused_metric"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := p.GetSeriesMetadata(context.Background(), SeriesMetadataParams{
				Page: 1, PageSize: 10, SortBy: "name", SortOrder: "asc", Type: "all", Usage: tt.usage,
			})
			assert.NoError(t, err, "GetSeriesMetadata")

			data, ok := res.Data.([]models.MetricMetadata)
			if !assert.True(t, ok, "Expected Data to be []models.MetricMetadata") {
				return
			}

			names := make([]string, 0, len(data))
			for _, metric := range data {
				names = append(names, metric.Name)
			}
			assert.ElementsMatch(t, tt.wantNames, names)
		})
	}
}

// testGetSeriesMetadataEmptyResults verifies GetSeriesMetadata's
// catalog-driven path (usage=all/used, as opposed to
// testGetSeriesMetadataUnusedJobScoped's unused-job-scoped path) returns an
// empty, error-free result against an empty catalog and against a filter
// that matches nothing, rather than erroring on the zero-row case.
func testGetSeriesMetadataEmptyResults(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	t.Run("empty catalog", func(t *testing.T) {
		for _, usage := range []string{SeriesMetadataUsageAll, SeriesMetadataUsageUsed} {
			t.Run(usage, func(t *testing.T) {
				out, err := p.GetSeriesMetadata(context.Background(), SeriesMetadataParams{
					Page: 1, PageSize: 10, SortBy: "name", SortOrder: "asc", Type: "all", Usage: usage,
				})
				assert.NoError(t, err)
				assert.Equal(t, 0, out.Total)
				if data, ok := out.Data.([]models.MetricMetadata); assert.True(t, ok, "type conversion") {
					assert.Empty(t, data)
				}
			})
		}
	})

	t.Run("filter matches nothing", func(t *testing.T) {
		mustUpsertCatalog(t, p, []MetricCatalogItem{{Name: "some_metric", Type: "gauge", Help: "h"}})
		out, err := p.GetSeriesMetadata(context.Background(), SeriesMetadataParams{
			Page: 1, PageSize: 10, SortBy: "name", SortOrder: "asc", Type: "all", Usage: SeriesMetadataUsageAll, Filter: "does_not_exist",
		})
		assert.NoError(t, err)
		assert.Equal(t, 0, out.Total)
		if data, ok := out.Data.([]models.MetricMetadata); assert.True(t, ok, "type conversion") {
			assert.Empty(t, data)
		}
	})
}

// testGetSeriesMetadataUnusedJobScoped exercises the ?usage=unused&job=<X>
// path (getSeriesMetadataUnusedJobScoped on both backends): driving the
// job-scoped case from the job index rather than the full unused universe
// keeps a request for a sparse job-match scaling with the requested job's
// own metric count, not the entire unused set. This pins the happy path:
// only metrics that are both unused AND tagged under the requested job come
// back, a metric unused under a different job is excluded, and a used
// metric tagged under the requested job is also excluded.
func testGetSeriesMetadataUnusedJobScoped(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	now := time.Now().UTC()
	mustUpsertCatalog(t, p, []MetricCatalogItem{
		{Name: "job_a_unused", Type: "gauge", Help: "unused, tagged under job a"},
		{Name: "job_a_used", Type: "gauge", Help: "used, tagged under job a"},
		{Name: "job_b_unused", Type: "gauge", Help: "unused, tagged under job b"},
	})
	mustUpsertJobIndex(t, p, []MetricJobIndexItem{
		{Name: "job_a_unused", Job: "job-a"},
		{Name: "job_a_used", Job: "job-a"},
		{Name: "job_b_unused", Job: "job-b"},
	})
	mustInsertQueries(t, p, []Query{{
		TS:            now.Add(-5 * time.Minute),
		QueryParam:    "job_a_used",
		TimeParam:     now,
		Duration:      10 * time.Millisecond,
		StatusCode:    200,
		LabelMatchers: LabelMatchers{{"__name__": "job_a_used"}},
		Type:          QueryTypeInstant,
	}})
	assert.NoError(t, p.RefreshMetricsUsageSummary(context.Background(), TimeRange{From: now.Add(-1 * time.Hour), To: now}))

	res, err := p.GetSeriesMetadata(context.Background(), SeriesMetadataParams{
		Page: 1, PageSize: 10, SortBy: "name", SortOrder: "asc", Type: "all", Usage: SeriesMetadataUsageUnused, Job: "job-a",
	})
	assert.NoError(t, err, "GetSeriesMetadata")

	data, ok := res.Data.([]models.MetricMetadata)
	if !assert.True(t, ok, "Expected Data to be []models.MetricMetadata") {
		return
	}
	names := make([]string, 0, len(data))
	for _, metric := range data {
		names = append(names, metric.Name)
	}
	// job_a_used is excluded (used); job_b_unused is excluded (wrong job).
	assert.ElementsMatch(t, []string{"job_a_unused"}, names)
	assert.Equal(t, 1, res.Total)

	t.Run("job with no unused metrics returns empty, not an error", func(t *testing.T) {
		res, err := p.GetSeriesMetadata(context.Background(), SeriesMetadataParams{
			Page: 1, PageSize: 10, SortBy: "name", SortOrder: "asc", Type: "all", Usage: SeriesMetadataUsageUnused, Job: "job-with-nothing-unused",
		})
		assert.NoError(t, err, "GetSeriesMetadata")
		assert.Equal(t, 0, res.Total)
	})
}

// testUpsertMetricsCatalogCreatesDefaultUnusedSummaryRow pins two invariants:
// (1) after UpsertMetricsCatalog, every catalog row has a corresponding
// metrics_usage_summary row with zero counts, even before any
// RefreshMetricsUsageSummary - this lets the unused query INNER JOIN safely
// instead of falling back to a LEFT JOIN + IS NULL branch; and (2) that
// placeholder row must NOT be marked is_unused=TRUE, since a metric that has
// never been evaluated is not the same thing as a metric confirmed to have
// zero usage. See https://github.com/nicolastakashi/prom-analytics-proxy/issues/570.
func testUpsertMetricsCatalogCreatesDefaultUnusedSummaryRow(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	mustUpsertCatalog(t, p, []MetricCatalogItem{
		{Name: "metric_a", Type: "gauge", Help: "a"},
		{Name: "metric_b", Type: "counter", Help: "b"},
	})

	type summaryRow struct {
		name                              string
		alert, record, dashboard, queries int
		isUnused                          bool
	}
	var got []summaryRow
	p.WithDB(func(d *sql.DB) {
		rows, err := d.QueryContext(context.Background(),
			`SELECT name, alert_count, record_count, dashboard_count, query_count, is_unused FROM metrics_usage_summary ORDER BY name`)
		assert.NoError(t, err, "query summary")
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var r summaryRow
			assert.NoError(t, rows.Scan(&r.name, &r.alert, &r.record, &r.dashboard, &r.queries, &r.isUnused))
			got = append(got, r)
		}
		assert.NoError(t, rows.Err())
	})

	assert.Equal(t, []summaryRow{
		{name: "metric_a", isUnused: false},
		{name: "metric_b", isUnused: false},
	}, got)
}

// testGetSeriesMetadataByNamesPopulatesIsUnused guards the write-path half of
// https://github.com/nicolastakashi/prom-analytics-proxy/issues/571:
// GetSeriesMetadataByNames (used by the OTLP ingester's usage-unused lookup)
// must source IsUnused from metrics_usage_summary.is_unused directly, not
// recompute it from the four usage counts - counts alone cannot distinguish
// "evaluated, confirmed zero usage" from "never evaluated yet".
func testGetSeriesMetadataByNamesPopulatesIsUnused(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	now := time.Now().UTC()
	mustUpsertCatalog(t, p, []MetricCatalogItem{
		{Name: "evaluated_unused_metric", Type: "gauge", Help: "evaluated, confirmed unused"},
	})
	// Evaluate: no usage anywhere for this metric, so RefreshMetricsUsageSummary
	// confirms it as genuinely unused.
	assert.NoError(t, p.RefreshMetricsUsageSummary(context.Background(), TimeRange{From: now.Add(-1 * time.Hour), To: now}))

	// Catalogued after the only refresh run: never evaluated.
	mustUpsertCatalog(t, p, []MetricCatalogItem{
		{Name: "fresh_metric", Type: "gauge", Help: "never evaluated"},
	})

	results, err := p.GetSeriesMetadataByNames(context.Background(), []string{"evaluated_unused_metric", "fresh_metric"}, "")
	assert.NoError(t, err, "GetSeriesMetadataByNames")

	byName := make(map[string]models.MetricMetadata, len(results))
	for _, mm := range results {
		byName[mm.Name] = mm
	}

	if assert.Contains(t, byName, "evaluated_unused_metric") {
		assert.True(t, byName["evaluated_unused_metric"].IsUnused, "a metric confirmed unused by RefreshMetricsUsageSummary must report IsUnused=true")
	}
	if assert.Contains(t, byName, "fresh_metric") {
		assert.False(t, byName["fresh_metric"].IsUnused, "a never-evaluated metric must not report IsUnused=true merely because its counts are zero")
	}
}

func testHistogramSummaryMetricsCatalog(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	// Histogram metrics: base metric should generate _bucket, _count, _sum variants
	histogramItems := []MetricCatalogItem{
		{Name: "access_evaluation_duration_bucket", Type: "histogram_bucket", Help: "Access evaluation duration (histogram buckets)", Unit: "seconds"},
		{Name: "access_evaluation_duration_count", Type: "histogram_count", Help: "Access evaluation duration (histogram count)", Unit: ""},
		{Name: "access_evaluation_duration_sum", Type: "histogram_sum", Help: "Access evaluation duration (histogram sum)", Unit: "seconds"},
	}
	// Summary metrics: base metric should be kept, plus _count, _sum variants
	summaryItems := []MetricCatalogItem{
		{Name: "request_latency", Type: "summary", Help: "Request latency", Unit: "seconds"},
		{Name: "request_latency_count", Type: "summary_count", Help: "Request latency (summary count)", Unit: ""},
		{Name: "request_latency_sum", Type: "summary_sum", Help: "Request latency (summary sum)", Unit: "seconds"},
	}
	allItems := append(histogramItems, summaryItems...)
	err := p.UpsertMetricsCatalog(context.Background(), allItems)
	assert.NoError(t, err, "UpsertMetricsCatalog")

	// Verify all metrics were stored correctly
	res, err := p.GetSeriesMetadata(context.Background(), SeriesMetadataParams{
		Page: 1, PageSize: 10, SortBy: "name", SortOrder: "asc", Type: "all",
	})
	assert.NoError(t, err, "GetSeriesMetadata")
	// Should have 6 metrics total (3 histogram + 3 summary)
	assert.Equal(t, 6, res.Total, "Expected 6 metrics")

	expectedHistogramMetrics := map[string]string{
		"access_evaluation_duration_bucket": "histogram_bucket",
		"access_evaluation_duration_count":  "histogram_count",
		"access_evaluation_duration_sum":    "histogram_sum",
	}
	expectedSummaryMetrics := map[string]string{
		"request_latency":       "summary",
		"request_latency_count": "summary_count",
		"request_latency_sum":   "summary_sum",
	}

	foundMetrics := make(map[string]string)
	if data, ok := res.Data.([]models.MetricMetadata); ok {
		for _, metric := range data {
			foundMetrics[metric.Name] = metric.Type
		}
	} else {
		assert.Fail(t, "Expected Data to be []models.MetricMetadata", "got %T", res.Data)
		return
	}

	for name, expectedType := range expectedHistogramMetrics {
		if actualType, found := foundMetrics[name]; !found {
			assert.Fail(t, "Histogram metric not found", "metric: %s", name)
		} else {
			assert.Equal(t, expectedType, actualType, "Histogram metric %s type", name)
		}
	}
	for name, expectedType := range expectedSummaryMetrics {
		if actualType, found := foundMetrics[name]; !found {
			assert.Fail(t, "Summary metric not found", "metric: %s", name)
		} else {
			assert.Equal(t, expectedType, actualType, "Summary metric %s type", name)
		}
	}

	histogramRes, err := p.GetSeriesMetadata(context.Background(), SeriesMetadataParams{
		Page: 1, PageSize: 10, SortBy: "name", SortOrder: "asc", Type: "histogram",
	})
	assert.NoError(t, err, "GetSeriesMetadata histogram filter")
	assert.Equal(t, 3, histogramRes.Total, "Expected 3 histogram metrics")
	if data, ok := histogramRes.Data.([]models.MetricMetadata); ok {
		if assert.Len(t, data, 3) {
			assert.ElementsMatch(t,
				[]string{"histogram_bucket", "histogram_count", "histogram_sum"},
				[]string{data[0].Type, data[1].Type, data[2].Type},
			)
		}
	} else {
		assert.Fail(t, "Expected histogram Data to be []models.MetricMetadata", "got %T", histogramRes.Data)
	}

	summaryRes, err := p.GetSeriesMetadata(context.Background(), SeriesMetadataParams{
		Page: 1, PageSize: 10, SortBy: "name", SortOrder: "asc", Type: "summary",
	})
	assert.NoError(t, err, "GetSeriesMetadata summary filter")
	assert.Equal(t, 3, summaryRes.Total, "Expected 3 summary metrics")
	if data, ok := summaryRes.Data.([]models.MetricMetadata); ok {
		if assert.Len(t, data, 3) {
			assert.ElementsMatch(t,
				[]string{"summary", "summary_count", "summary_sum"},
				[]string{data[0].Type, data[1].Type, data[2].Type},
			)
		}
	} else {
		assert.Fail(t, "Expected summary Data to be []models.MetricMetadata", "got %T", summaryRes.Data)
	}
}

func testRefreshMetricsUsageSummaryAndGetSeriesMetadata(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	// Seed catalog and job index
	mustUpsertCatalog(t, p, []MetricCatalogItem{{Name: "up", Type: "gauge", Help: "up metric"}})
	mustUpsertJobIndex(t, p, []MetricJobIndexItem{{Name: "up", Job: "prometheus"}})

	// Seed related data: queries, rules, dashboards within window
	now := time.Now().UTC()
	mustInsertQueries(t, p, []Query{{
		TS:            now.Add(-5 * time.Minute),
		QueryParam:    "up",
		TimeParam:     now,
		Duration:      10 * time.Millisecond,
		StatusCode:    200,
		LabelMatchers: LabelMatchers{{"__name__": "up"}},
		Type:          QueryTypeInstant,
	}})
	mustInsertRules(t, p, []RulesUsage{{
		Serie:      "up",
		GroupName:  "default",
		Name:       "up_alert",
		Expression: "up == 0",
		Kind:       string(RuleUsageKindAlert),
		Labels:     []string{"severity"},
		CreatedAt:  now.Add(-10 * time.Minute),
	}})
	mustInsertDashboards(t, p, []DashboardUsage{{
		Id:        "dash1",
		Serie:     "up",
		Name:      "Up Overview",
		URL:       "http://example/d/dash1",
		CreatedAt: now.Add(-15 * time.Minute),
	}})

	assert.NoError(t, p.RefreshMetricsUsageSummary(context.Background(), TimeRange{From: now.Add(-1 * time.Hour), To: now}))

	res, err := p.GetSeriesMetadata(context.Background(), SeriesMetadataParams{
		Page: 1, PageSize: 10, SortBy: "name", SortOrder: "asc", Filter: "up", Type: "all", Job: "prometheus",
	})
	assert.NoError(t, err, "GetSeriesMetadata")
	assert.Greater(t, res.Total, 0)
}

// testWriteMethodsFailCleanlyOnCancelledContext verifies that every
// transactional write method - Insert, UpsertMetricsCatalog,
// UpsertMetricsJobIndex, InsertRulesUsage, and InsertDashboardUsage - opens
// its transaction before doing anything else, by pre-cancelling the context
// so BeginTx fails immediately: each method must surface that failure as an
// error rather than reaching the network or disk.
func testWriteMethodsFailCleanlyOnCancelledContext(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("Insert", func(t *testing.T) {
		err := p.Insert(ctx, []Query{{QueryParam: "up", Type: QueryTypeInstant}})
		assert.Error(t, err)
	})
	t.Run("UpsertMetricsCatalog", func(t *testing.T) {
		err := p.UpsertMetricsCatalog(ctx, []MetricCatalogItem{{Name: "m", Type: "gauge"}})
		assert.Error(t, err)
	})
	t.Run("UpsertMetricsJobIndex", func(t *testing.T) {
		err := p.UpsertMetricsJobIndex(ctx, []MetricJobIndexItem{{Name: "m", Job: "j"}})
		assert.Error(t, err)
	})
	t.Run("InsertRulesUsage", func(t *testing.T) {
		err := p.InsertRulesUsage(ctx, []RulesUsage{{
			Serie: "s", GroupName: "g", Name: "n", Expression: "e",
			Kind: string(RuleUsageKindAlert), CreatedAt: time.Now().UTC(),
		}})
		assert.Error(t, err)
	})
	t.Run("InsertDashboardUsage", func(t *testing.T) {
		err := p.InsertDashboardUsage(ctx, []DashboardUsage{{
			Id: "d", Serie: "s", Name: "n", URL: "u", CreatedAt: time.Now().UTC(),
		}})
		assert.Error(t, err)
	})
}

// testWriteMethodsNoOpOnEmptyInput verifies that every transactional write
// method - Insert, UpsertMetricsCatalog, UpsertMetricsJobIndex,
// InsertRulesUsage, and InsertDashboardUsage - returns nil without opening a
// transaction when given a zero-length slice, rather than erroring or
// executing a no-op transaction.
func testWriteMethodsNoOpOnEmptyInput(t *testing.T, newProvider newProviderFunc) {
	p, cleanup := newProvider(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("Insert", func(t *testing.T) {
		assert.NoError(t, p.Insert(ctx, []Query{}))
	})
	t.Run("UpsertMetricsCatalog", func(t *testing.T) {
		assert.NoError(t, p.UpsertMetricsCatalog(ctx, []MetricCatalogItem{}))
	})
	t.Run("UpsertMetricsJobIndex", func(t *testing.T) {
		assert.NoError(t, p.UpsertMetricsJobIndex(ctx, []MetricJobIndexItem{}))
	})
	t.Run("InsertRulesUsage", func(t *testing.T) {
		assert.NoError(t, p.InsertRulesUsage(ctx, []RulesUsage{}))
	})
	t.Run("InsertDashboardUsage", func(t *testing.T) {
		assert.NoError(t, p.InsertDashboardUsage(ctx, []DashboardUsage{}))
	})
}
