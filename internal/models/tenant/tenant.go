package tenant

// Tenant Definition
type Tenant struct {
	ID             int64 `gorm:"primaryKey"`
	Name           string
	ExternalTenant string
	Description    string
}
