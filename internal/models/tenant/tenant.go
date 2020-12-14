package tenant

import "time"

type Tenant struct {
	ID             int64 `gorm:"primaryKey"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Name           string
	ExternalTenant string
	Description    string
}
