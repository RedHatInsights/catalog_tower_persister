package main

import (
	"context"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredential"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceoffering"
)

//DeleteHandler stores the PageContext used to delete unwanted objects
type DeleteHandler struct {
	pageContext *PageContext
}

//ProcessDeletes deletes unwanted objects
func (pc *PageContext) ProcessDeletes(ctx context.Context) error {
	if len(pc.jobTemplateSourceRefs) > 0 {
		so := &serviceoffering.ServiceOffering{SourceID: pc.source.ID, TenantID: pc.tenant.ID}
		if err := pc.repos.serviceofferingrepo.DeleteUnwanted(ctx, pc.logger, so, pc.jobTemplateSourceRefs, pc.repos.serviceplanrepo); err != nil {
			pc.logger.Errorf("Error deleting Service Offering %v", err)
			return err
		}
	}

	if len(pc.inventorySourceRefs) > 0 {
		si := &serviceinventory.ServiceInventory{SourceID: pc.source.ID, TenantID: pc.tenant.ID}
		if err := pc.repos.serviceinventoryrepo.DeleteUnwanted(ctx, pc.logger, si, pc.inventorySourceRefs); err != nil {
			pc.logger.Errorf("Error deleting Service Inventories %v", err)
			return err
		}
	}

	if len(pc.credentialSourceRefs) > 0 {
		sc := &servicecredential.ServiceCredential{SourceID: pc.source.ID, TenantID: pc.tenant.ID}
		if err := pc.repos.servicecredentialrepo.DeleteUnwanted(ctx, pc.logger, sc, pc.credentialSourceRefs); err != nil {
			pc.logger.Errorf("Error deleting Service Credentials %v", err)
			return err
		}
	}

	if len(pc.credentialTypeSourceRefs) > 0 {
		sct := &servicecredentialtype.ServiceCredentialType{SourceID: pc.source.ID, TenantID: pc.tenant.ID}
		if err := pc.repos.servicecredentialtyperepo.DeleteUnwanted(ctx, pc.logger, sct, pc.credentialTypeSourceRefs); err != nil {
			pc.logger.Errorf("Error deleting Service credential types %v", err)
			return err
		}
	}
	return nil
}
