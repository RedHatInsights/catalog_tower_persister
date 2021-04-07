package base

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

//ResultIDRef stores the DB ID and the external tower id
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

// Tower Attributes that we use across all objects
type Tower struct {
	SourceRef       string
	SourceCreatedAt time.Time
	SourceUpdatedAt time.Time
	// SourceDeletedAt  sql.NullTime
	LastSeenAt sql.NullTime
}

//TowerTime converts datetime from Tower to UTC
func TowerTime(str string) (time.Time, error) {
	//"2020-01-08T10:22:59.423585Z"
	// Drop the subseconds
	s := fmt.Sprintf("%sZ", strings.Split(str, ".")[0])
	t, err := time.Parse(time.RFC3339, s)
	return t, err
}

//SourceRefExists check if a tower id defined in srcRef exists in an array of sorted
//Tower ids. This allows us to figure out the deletes
func SourceRefExists(srcRef string, sortedRefs []string, length int) bool {
	i := sort.SearchStrings(sortedRefs, srcRef)
	if i >= length || sortedRefs[i] != srcRef {
		return false
	}
	return true
}
