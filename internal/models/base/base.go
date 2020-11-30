package base

import (
	"database/sql"
	"sort"
	"time"

	"gorm.io/gorm"
)

type JSONB map[string]interface{}

type ResultIDRef struct {
	ID        int64
	SourceRef string
}

// Base definition
type Base struct {
	ID         int64 `gorm:"primaryKey"`
	CreatedAt  time.Time
	UpdatedAt  time.Time
	ArchivedAt gorm.DeletedAt `gorm:"index"`
}

type Tower struct {
	SourceRef       string
	SourceCreatedAt time.Time
	// SourceUpdatedAt  time.Time
	// SourceDeletedAt  sql.NullTime
	LastSeenAt sql.NullTime
}

func TowerTime(str string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, str)
	return t, err
}

func SourceRefExists(srcRef string, sortedRefs []string, length int) bool {
	i := sort.SearchStrings(sortedRefs, srcRef)
	if i >= length || sortedRefs[i] != srcRef {
		return false
	}
	return true
}
