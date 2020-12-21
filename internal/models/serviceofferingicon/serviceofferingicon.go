package serviceofferingicon

import (
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/base"
)

type ServiceOfferingIcon struct {
	base.Base
	base.Tower
	Data     []byte
	TenantID int64
	SourceID int64
}
