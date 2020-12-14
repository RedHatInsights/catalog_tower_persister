package source

import (
	"database/sql"
	"fmt"
	"time"
)

type Source struct {
	ID                      int64 `gorm:"primaryKey"`
	CreatedAt               time.Time
	UpdatedAt               time.Time
	TenantID                int64
	UID                     string
	RefreshState            string
	BytesReceived           int64
	RefreshStartedAt        sql.NullTime
	RefreshFinishedAt       sql.NullTime
	LastSuccessfulRefreshAt sql.NullTime
}

func (s Source) String() string {
	return fmt.Sprintf("state=%v payload size=%d start=%v finished=%v",
		s.RefreshState,
		s.BytesReceived,
		s.RefreshStartedAt,
		s.RefreshFinishedAt)
}
