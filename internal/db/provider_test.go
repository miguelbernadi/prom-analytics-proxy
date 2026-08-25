package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValidateSQLQuery(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name:        "valid query",
			query:       "SELECT * FROM queries WHERE metric_name = 'up'",
			expectError: false,
		},
		{
			name:        "empty query",
			query:       "",
			expectError: false,
		},
		{
			name:        "contains DROP",
			query:       "DROP TABLE queries",
			expectError: true,
		},
		{
			name:        "contains DELETE",
			query:       "DELETE FROM queries",
			expectError: true,
		},
		{
			name:        "contains UPDATE",
			query:       "UPDATE queries SET status = 200",
			expectError: true,
		},
		{
			name:        "contains INSERT",
			query:       "INSERT INTO queries VALUES (1, 'up')",
			expectError: true,
		},
		{
			name:        "contains ALTER",
			query:       "ALTER TABLE queries ADD COLUMN new_field",
			expectError: true,
		},
		{
			name:        "contains TRUNCATE",
			query:       "TRUNCATE TABLE queries",
			expectError: true,
		},
		{
			name:        "contains EXEC",
			query:       "EXEC stored_procedure",
			expectError: true,
		},
		{
			name:        "case insensitive DROP",
			query:       "drop table queries",
			expectError: true,
		},
		{
			name:        "mixed case DELETE",
			query:       "DeLeTe from queries",
			expectError: true,
		},
		{
			name:        "contains comment",
			query:       "SELECT * FROM queries -- comment",
			expectError: true,
		},
		{
			name:        "contains semicolon",
			query:       "SELECT * FROM queries;",
			expectError: true,
		},
		{
			name:        "contains multiple semicolons",
			query:       "SELECT * FROM queries; SELECT * FROM metrics;",
			expectError: true,
		},
		{
			name:        "contains comment and semicolon",
			query:       "SELECT * FROM queries -- comment;",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSQLQuery(tt.query)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateSortField(t *testing.T) {
	tests := []struct {
		name           string
		sortBy         string
		sortOrder      string
		validFields    map[string]bool
		defaultSort    string
		expectedSortBy string
		expectedOrder  string
	}{
		{
			name:           "valid sort field and order",
			sortBy:         "timestamp",
			sortOrder:      "desc",
			validFields:    map[string]bool{"timestamp": true, "duration": true},
			defaultSort:    "timestamp",
			expectedSortBy: "timestamp",
			expectedOrder:  "desc",
		},
		{
			name:           "empty sort by",
			sortBy:         "",
			sortOrder:      "asc",
			validFields:    map[string]bool{"timestamp": true, "duration": true},
			defaultSort:    "timestamp",
			expectedSortBy: "timestamp",
			expectedOrder:  "asc",
		},
		{
			name:           "empty sort order",
			sortBy:         "duration",
			sortOrder:      "",
			validFields:    map[string]bool{"timestamp": true, "duration": true},
			defaultSort:    "timestamp",
			expectedSortBy: "duration",
			expectedOrder:  "desc",
		},
		{
			name:           "invalid sort field",
			sortBy:         "invalid_field",
			sortOrder:      "desc",
			validFields:    map[string]bool{"timestamp": true, "duration": true},
			defaultSort:    "timestamp",
			expectedSortBy: "timestamp",
			expectedOrder:  "desc",
		},
		{
			name:           "both empty",
			sortBy:         "",
			sortOrder:      "",
			validFields:    map[string]bool{"timestamp": true, "duration": true},
			defaultSort:    "timestamp",
			expectedSortBy: "timestamp",
			expectedOrder:  "desc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sortBy := tt.sortBy
			sortOrder := tt.sortOrder

			ValidateSortField(&sortBy, &sortOrder, tt.validFields, tt.defaultSort)

			assert.Equal(t, tt.expectedSortBy, sortBy)
			assert.Equal(t, tt.expectedOrder, sortOrder)
		})
	}
}

// TestResolveSafeSortExpr covers every fallback tier of resolveSafeSortExpr.
// This is a SQL-injection-prevention control (see its doc comment: the
// returned string is always a static literal or a map lookup, never the
// caller's sortBy string interpolated directly) - its fallback tiers exist
// specifically for callers with incomplete alias maps or absent table
// aliases, so each tier needs its own case rather than relying on whatever
// combination the current callers happen to exercise.
func TestResolveSafeSortExpr(t *testing.T) {
	aliases := map[string]string{
		"queryCount": "COALESCE(s.query_count, 0)",
		"name":       "c.name",
	}

	tests := []struct {
		name        string
		sortBy      string
		tableAlias  string
		defaultSort string
		sortAliases []map[string]string
		want        string
	}{
		{
			name:        "sortBy found directly in sortAliases",
			sortBy:      "queryCount",
			tableAlias:  "c",
			defaultSort: "name",
			sortAliases: []map[string]string{aliases},
			want:        "COALESCE(s.query_count, 0)",
		},
		{
			name:        "sortBy absent, defaultSort found in sortAliases",
			sortBy:      "unknownField",
			tableAlias:  "c",
			defaultSort: "name",
			sortAliases: []map[string]string{aliases},
			want:        "c.name",
		},
		{
			name:        "sortAliases provided but neither key matches - falls through to tableAlias.defaultSort",
			sortBy:      "unknownField",
			tableAlias:  "x",
			defaultSort: "otherField",
			sortAliases: []map[string]string{aliases},
			want:        "x.otherField",
		},
		{
			name:        "no sortAliases, tableAlias and defaultSort both set",
			sortBy:      "anything",
			tableAlias:  "c",
			defaultSort: "name",
			sortAliases: nil,
			want:        "c.name",
		},
		{
			name:        "nil map inside sortAliases is treated as absent",
			sortBy:      "queryCount",
			tableAlias:  "c",
			defaultSort: "name",
			sortAliases: []map[string]string{nil},
			want:        "c.name",
		},
		{
			name:        "no sortAliases, no tableAlias, defaultSort set",
			sortBy:      "anything",
			tableAlias:  "",
			defaultSort: "name",
			sortAliases: nil,
			want:        "name",
		},
		{
			name:        "nothing set at all falls back to the literal 1",
			sortBy:      "",
			tableAlias:  "",
			defaultSort: "",
			sortAliases: nil,
			want:        "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveSafeSortExpr(tt.sortBy, tt.tableAlias, tt.defaultSort, tt.sortAliases...)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidatePagination(t *testing.T) {
	tests := []struct {
		name            string
		page            int
		pageSize        int
		defaultPageSize int
		expectedPage    int
		expectedSize    int
	}{
		{
			name:            "valid pagination",
			page:            2,
			pageSize:        20,
			defaultPageSize: 10,
			expectedPage:    2,
			expectedSize:    20,
		},
		{
			name:            "zero page",
			page:            0,
			pageSize:        20,
			defaultPageSize: 10,
			expectedPage:    1,
			expectedSize:    20,
		},
		{
			name:            "negative page",
			page:            -1,
			pageSize:        20,
			defaultPageSize: 10,
			expectedPage:    1,
			expectedSize:    20,
		},
		{
			name:            "zero page size",
			page:            1,
			pageSize:        0,
			defaultPageSize: 10,
			expectedPage:    1,
			expectedSize:    10,
		},
		{
			name:            "negative page size",
			page:            1,
			pageSize:        -5,
			defaultPageSize: 10,
			expectedPage:    1,
			expectedSize:    10,
		},
		{
			name:            "both invalid",
			page:            0,
			pageSize:        0,
			defaultPageSize: 10,
			expectedPage:    1,
			expectedSize:    10,
		},
		{
			name:            "page size over MaxPageSize clamps to it",
			page:            1,
			pageSize:        MaxPageSize + 1,
			defaultPageSize: 10,
			expectedPage:    1,
			expectedSize:    MaxPageSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := tt.page
			pageSize := tt.pageSize

			ValidatePagination(&page, &pageSize, tt.defaultPageSize)

			assert.Equal(t, tt.expectedPage, page)
			assert.Equal(t, tt.expectedSize, pageSize)
		})
	}
}

// TestSetDefaultTimeRange verifies SetDefaultTimeRange fills in only the
// zero-valued end(s) of the range, defaulting From to 30 days before now and
// To to now, and leaves an explicitly-set From/To untouched.
func TestSetDefaultTimeRange(t *testing.T) {
	explicitFrom := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	explicitTo := time.Date(2020, 6, 1, 0, 0, 0, 0, time.UTC)

	t.Run("both zero get defaulted", func(t *testing.T) {
		tr := TimeRange{}
		SetDefaultTimeRange(&tr)

		now := time.Now().UTC()
		assert.WithinDuration(t, now.Add(-ThirtyDays), tr.From, time.Minute)
		assert.WithinDuration(t, now, tr.To, time.Minute)
	})

	t.Run("only From zero gets defaulted, To is untouched", func(t *testing.T) {
		tr := TimeRange{To: explicitTo}
		SetDefaultTimeRange(&tr)

		assert.WithinDuration(t, time.Now().UTC().Add(-ThirtyDays), tr.From, time.Minute)
		assert.Equal(t, explicitTo, tr.To)
	})

	t.Run("only To zero gets defaulted, From is untouched", func(t *testing.T) {
		tr := TimeRange{From: explicitFrom}
		SetDefaultTimeRange(&tr)

		assert.Equal(t, explicitFrom, tr.From)
		assert.WithinDuration(t, time.Now().UTC(), tr.To, time.Minute)
	})

	t.Run("neither zero, both untouched", func(t *testing.T) {
		tr := TimeRange{From: explicitFrom, To: explicitTo}
		SetDefaultTimeRange(&tr)

		assert.Equal(t, explicitFrom, tr.From)
		assert.Equal(t, explicitTo, tr.To)
	})
}

func TestCalculateTotalPages(t *testing.T) {
	tests := []struct {
		name       string
		totalCount int
		pageSize   int
		expected   int
	}{
		{
			name:       "exact division",
			totalCount: 100,
			pageSize:   10,
			expected:   10,
		},
		{
			name:       "remainder",
			totalCount: 105,
			pageSize:   10,
			expected:   11,
		},
		{
			name:       "single page",
			totalCount: 5,
			pageSize:   10,
			expected:   1,
		},
		{
			name:       "empty result",
			totalCount: 0,
			pageSize:   10,
			expected:   0,
		},
		{
			name:       "page size larger than total",
			totalCount: 5,
			pageSize:   20,
			expected:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateTotalPages(tt.totalCount, tt.pageSize)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTimeRange_Format(t *testing.T) {
	tr := TimeRange{
		From: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		To:   time.Date(2023, 1, 15, 12, 0, 0, 0, time.UTC),
	}

	fromStr, toStr := tr.Format(time.RFC3339)

	expectedFrom := "2023-01-01T12:00:00Z"
	expectedTo := "2023-01-15T12:00:00Z"

	assert.Equal(t, expectedFrom, fromStr)
	assert.Equal(t, expectedTo, toStr)
}

func TestTimeRange_Previous(t *testing.T) {
	tr := TimeRange{
		From: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		To:   time.Date(2023, 1, 15, 12, 0, 0, 0, time.UTC),
	}

	previous := tr.Previous()

	expectedFrom := time.Date(2022, 12, 18, 12, 0, 0, 0, time.UTC)
	expectedTo := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

	assert.Equal(t, expectedFrom, previous.From)
	assert.Equal(t, expectedTo, previous.To)
}

func TestGetDbProvider(t *testing.T) {
	tests := []struct {
		name        string
		provider    DatabaseProvider
		expectError bool
	}{
		{
			name:        "postgresql provider",
			provider:    PostGreSQL,
			expectError: true, // Will fail due to missing database connection
		},
		{
			name:        "sqlite provider",
			provider:    SQLite,
			expectError: false, // Can succeed by creating a database file
		},
		{
			name:        "invalid provider",
			provider:    "invalid",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, err := GetDbProvider(context.Background(), tt.provider)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, provider)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, provider)
			}
		})
	}
}
