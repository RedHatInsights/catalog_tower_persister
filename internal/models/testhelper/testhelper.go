package testhelper

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// AnyTime is an empty struct to match any time in DB records
type AnyTime struct{}

// Match satisfies sqlmock.Argument interface
func (a AnyTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)
	return ok
}

// AnyJSONB is an empty map[string]interface{} to match JSONB

type AnyJSONB map[string]interface{}

// Match satisfies sqlmock.Argument interface
func (a AnyJSONB) Match(v driver.Value) bool {
	_, ok := v.(map[string]interface{})
	return ok
}

// MockDBSetup creates a mock DB to be used with Gorm
func MockDBSetup(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	assert.Nilf(t, err, "error opening stub database %v", err)
	gdb, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})

	assert.Nilf(t, err, "error opening gorm postgres database %v", err)
	teardown := func() {
		db.Close()
	}
	return gdb, mock, teardown
}

func TestLogger() *logrus.Entry {
	logger := logrus.New()
	return logger.WithFields(logrus.Fields{"request_id": "7888888"})
}
