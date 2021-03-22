package payload

import (
	"context"
	"fmt"
)

// Reporter Interface returns a summary of what database changes were performed
type Reporter interface {
	GetStats(ctx context.Context) map[string]interface{}
}

// GetStats get counters for objects added/updated/deleted which can be set back to the
// Catalog Inventory API
func (bol *BillOfLading) GetStats(ctx context.Context) map[string]interface{} {
	bol.logReports(ctx)
	stats := map[string]interface{}{
		"credentials":            bol.repos.servicecredentialrepo.Stats(),
		"credential_types":       bol.repos.servicecredentialrepo.Stats(),
		"inventories":            bol.repos.serviceinventoryrepo.Stats(),
		"service_plans":          bol.repos.serviceplanrepo.Stats(),
		"service_offering":       bol.repos.serviceofferingrepo.Stats(),
		"service_offering_nodes": bol.repos.serviceofferingnoderepo.Stats(),
	}
	return stats
}

// logReports log the objects added/updated/deleted
func (bol *BillOfLading) logReports(ctx context.Context) {
	x := bol.repos.servicecredentialrepo.Stats()
	bol.logger.Info(fmt.Sprintf("Credential Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = bol.repos.servicecredentialrepo.Stats()
	bol.logger.Info(fmt.Sprintf("Credential Type Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = bol.repos.serviceinventoryrepo.Stats()
	bol.logger.Info(fmt.Sprintf("Inventory Type Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = bol.repos.serviceplanrepo.Stats()
	bol.logger.Info(fmt.Sprintf("Service Plan Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = bol.repos.serviceofferingrepo.Stats()
	bol.logger.Info(fmt.Sprintf("Service Offering Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = bol.repos.serviceofferingnoderepo.Stats()
	bol.logger.Info(fmt.Sprintf("Service Offering Node Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
}
