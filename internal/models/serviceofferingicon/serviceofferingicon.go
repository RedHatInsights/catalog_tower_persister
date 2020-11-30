package serviceofferingicon

import (
	"github.com/mkanoor/catalog_tower_persister/internal/models/base"
	"github.com/mkanoor/catalog_tower_persister/internal/models/source"
	"github.com/mkanoor/catalog_tower_persister/internal/models/tenant"
)

type ServiceOfferingIcon struct {
	base.Base
	base.Tower
	Data     []byte
	TenantID int64
	SourceID int64
	Tenant   tenant.Tenant
	Source   source.Source
}
