package source

import (
	"database/sql"
	"fmt"

	"github.com/mkanoor/catalog_tower_persister/internal/models/base"
	"github.com/mkanoor/catalog_tower_persister/internal/models/tenant"
)

type Source struct {
	base.Base
	TenantID                int64
	UID                     string
	RefreshStatus           string
	Tenant                  tenant.Tenant
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
