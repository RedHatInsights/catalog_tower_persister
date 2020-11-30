package tenant

import "github.com/mkanoor/catalog_tower_persister/internal/models/base"

type Tenant struct {
	base.Base
	Name           string
	ExternalTenant string
	Description    string
}
