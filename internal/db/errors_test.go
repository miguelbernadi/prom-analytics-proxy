package db

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidationError(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		message  string
		expected string
	}{
		{
			name:     "database type validation error",
			field:    "database type",
			message:  "invalid type 'mysql', only 'postgresql' and 'sqlite' are supported",
			expected: "validation error for database type: invalid type 'mysql', only 'postgresql' and 'sqlite' are supported",
		},
		{
			name:     "SQL query validation error",
			field:    "SQL",
			message:  "query contains disallowed keyword",
			expected: "validation error for SQL: query contains disallowed keyword",
		},
		{
			name:     "empty field and message",
			field:    "",
			message:  "",
			expected: "validation error for : ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidationError(tt.field, tt.message)
			assert.Equal(t, tt.expected, err.Error())
		})
	}
}

func TestErrorWithOperation(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		operation string
		expected  string
	}{
		{
			name:      "database connection error",
			err:       errors.New("connection refused"),
			operation: "connecting to database",
			expected:  "connecting to database: connection refused",
		},
		{
			name:      "query execution error",
			err:       errors.New("syntax error"),
			operation: "executing query",
			expected:  "executing query: syntax error",
		},
		{
			name:      "nil error",
			err:       nil,
			operation: "test operation",
			expected:  "test operation: <nil>",
		},
		{
			name:      "empty operation",
			err:       errors.New("test error"),
			operation: "",
			expected:  ": test error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ErrorWithOperation(tt.err, tt.operation)
			assert.Equal(t, tt.expected, err.Error())
		})
	}
}

func TestCloseResource(t *testing.T) {
	tests := []struct {
		name        string
		resource    io.Closer
		expectPanic bool
	}{
		{
			name:        "nil resource",
			resource:    nil,
			expectPanic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic {
				assert.Panics(t, func() {
					CloseResource(tt.resource)
				})
			} else {
				assert.NotPanics(t, func() {
					CloseResource(tt.resource)
				})
			}
		})
	}
}

func TestIsNoResults(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "no results error",
			err:      ErrNoResults,
			expected: true,
		},
		{
			name:     "other error",
			err:      errors.New("other error"),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsNoResults(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQueryError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		operation string
		details   string
		expected  string
	}{
		{
			name:      "with details",
			err:       errors.New("connection failed"),
			operation: "connecting",
			details:   "timeout after 30s",
			expected:  "connecting: timeout after 30s: connection failed",
		},
		{
			name:      "without details",
			err:       errors.New("connection failed"),
			operation: "connecting",
			details:   "",
			expected:  "connecting: connection failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := QueryError(tt.err, tt.operation, tt.details)
			assert.Equal(t, tt.expected, err.Error())
		})
	}
}

func TestConnectionError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		dbType   string
		details  string
		expected string
	}{
		{
			name:     "with details",
			err:      errors.New("connection refused"),
			dbType:   "postgresql",
			details:  "invalid credentials",
			expected: "failed to connect to postgresql database: connection refused: invalid credentials",
		},
		{
			name:     "without details",
			err:      errors.New("connection refused"),
			dbType:   "sqlite",
			details:  "",
			expected: "failed to connect to sqlite database: connection refused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ConnectionError(tt.err, tt.dbType, tt.details)
			assert.Equal(t, tt.expected, err.Error())
		})
	}
}

func TestSchemaError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		operation string
		table     string
		expected  string
	}{
		{
			name:      "create table error",
			err:       errors.New("table already exists"),
			operation: "create",
			table:     "queries",
			expected:  "schema create failed for table queries: table already exists",
		},
		{
			name:      "drop table error",
			err:       errors.New("table does not exist"),
			operation: "drop",
			table:     "metrics",
			expected:  "schema drop failed for table metrics: table does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SchemaError(tt.err, tt.operation, tt.table)
			assert.Equal(t, tt.expected, err.Error())
		})
	}
}

func TestErrorTypes_Unwrap(t *testing.T) {
	originalErr := errors.New("original error")

	validationErr := ValidationError("field", "message")
	assert.Error(t, validationErr)

	wrappedErr := ErrorWithOperation(originalErr, "operation")
	assert.Error(t, wrappedErr)
	assert.Contains(t, wrappedErr.Error(), "original error")

	queryErr := QueryError(originalErr, "query", "details")
	assert.Error(t, queryErr)
	assert.Contains(t, queryErr.Error(), "original error")
}

func TestErrorTypes_Comparison(t *testing.T) {
	err1 := ValidationError("field", "message")
	err2 := ValidationError("field", "message")
	err3 := ValidationError("different", "message")

	assert.Equal(t, err1.Error(), err2.Error())
	assert.NotEqual(t, err1.Error(), err3.Error())
}

func TestErrorTypes_Formatting(t *testing.T) {
	err := ValidationError("field name", "message with 'quotes' and \"double quotes\"")
	assert.Contains(t, err.Error(), "field name")
	assert.Contains(t, err.Error(), "message with 'quotes' and \"double quotes\"")
}

func TestErrorTypes_EmptyValues(t *testing.T) {
	validationErr := ValidationError("", "")
	assert.Equal(t, "validation error for : ", validationErr.Error())

	wrappedErr := ErrorWithOperation(nil, "")
	assert.Equal(t, ": <nil>", wrappedErr.Error())
}

func TestErrorTypes_UnicodeSupport(t *testing.T) {
	err := ValidationError("fält", "meddelande med åäö")
	assert.Contains(t, err.Error(), "fält")
	assert.Contains(t, err.Error(), "meddelande med åäö")
}

func TestErrorTypes_LongMessages(t *testing.T) {
	longMessage := "This is a very long error message that contains many characters and should be handled properly by the error system without any issues or truncation"
	err := ValidationError("field", longMessage)
	assert.Contains(t, err.Error(), longMessage)
	assert.Contains(t, err.Error(), "validation error for field:")
}

func TestStandardErrors(t *testing.T) {
	assert.Error(t, ErrNoResults)
	assert.Error(t, ErrInvalidScan)
	assert.Error(t, ErrInvalidQuery)
	assert.Error(t, ErrDatabaseConnection)
	assert.Error(t, ErrQueryExecution)

	assert.Equal(t, "no results found", ErrNoResults.Error())
	assert.Equal(t, "invalid row scan", ErrInvalidScan.Error())
	assert.Equal(t, "invalid query", ErrInvalidQuery.Error())
	assert.Equal(t, "database connection error", ErrDatabaseConnection.Error())
	assert.Equal(t, "query execution failed", ErrQueryExecution.Error())
}
