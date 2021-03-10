package source

import (
	"time"
)

// Source definition
type Source struct {
	ID        int64 `gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	TenantID  int64
}
