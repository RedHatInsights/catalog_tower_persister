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

//Process deletes unwanted objects
func (dh *DeleteHandler) Process(ctx context.Context) error {
	if len(dh.pageContext.jobTemplateSourceRefs) > 0 {
		so := &serviceoffering.ServiceOffering{SourceID: dh.pageContext.source.ID, TenantID: dh.pageContext.tenant.ID}
		if err := dh.pageContext.repos.serviceofferingrepo.DeleteUnwanted(ctx, dh.pageContext.logger, so, dh.pageContext.jobTemplateSourceRefs, dh.pageContext.repos.serviceplanrepo); err != nil {
			dh.pageContext.logger.Errorf("Error deleting Service Offering %v", err)
			return err
		}
	}

	if len(dh.pageContext.inventorySourceRefs) > 0 {
		si := &serviceinventory.ServiceInventory{SourceID: dh.pageContext.source.ID, TenantID: dh.pageContext.tenant.ID}
		if err := dh.pageContext.repos.serviceinventoryrepo.DeleteUnwanted(ctx, dh.pageContext.logger, si, dh.pageContext.inventorySourceRefs); err != nil {
			dh.pageContext.logger.Errorf("Error deleting Service Inventories %v", err)
			return err
		}
	}

	if len(dh.pageContext.credentialSourceRefs) > 0 {
		sc := &servicecredential.ServiceCredential{SourceID: dh.pageContext.source.ID, TenantID: dh.pageContext.tenant.ID}
		if err := dh.pageContext.repos.servicecredentialrepo.DeleteUnwanted(ctx, dh.pageContext.logger, sc, dh.pageContext.credentialSourceRefs); err != nil {
			dh.pageContext.logger.Errorf("Error deleting Service Credentials %v", err)
			return err
		}
	}

	if len(dh.pageContext.credentialTypeSourceRefs) > 0 {
		sct := &servicecredentialtype.ServiceCredentialType{SourceID: dh.pageContext.source.ID, TenantID: dh.pageContext.tenant.ID}
		if err := dh.pageContext.repos.servicecredentialtyperepo.DeleteUnwanted(ctx, dh.pageContext.logger, sct, dh.pageContext.credentialTypeSourceRefs); err != nil {
			dh.pageContext.logger.Errorf("Error deleting Service credential types %v", err)
			return err
		}
	}
	return nil
}
