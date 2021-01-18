package main

import (
	"context"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredential"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceoffering"
)

type DeleteHandler struct {
	PC *PageContext
}

func (dh *DeleteHandler) Process(ctx context.Context) error {
	if len(dh.PC.jobTemplateSourceRefs) > 0 {
		so := &serviceoffering.ServiceOffering{SourceID: dh.PC.Source.ID, TenantID: dh.PC.Tenant.ID}
		if err := dh.PC.serviceofferingrepo.DeleteUnwanted(ctx, dh.PC.logger, so, dh.PC.jobTemplateSourceRefs, dh.PC.serviceplanrepo); err != nil {
			dh.PC.logger.Errorf("Error deleting Service Offering %v", err)
			return err
		}
	}

	if len(dh.PC.inventorySourceRefs) > 0 {
		si := &serviceinventory.ServiceInventory{SourceID: dh.PC.Source.ID, TenantID: dh.PC.Tenant.ID}
		if err := dh.PC.serviceinventoryrepo.DeleteUnwanted(ctx, dh.PC.logger, si, dh.PC.inventorySourceRefs); err != nil {
			dh.PC.logger.Errorf("Error deleting Service Inventories %v", err)
			return err
		}
	}

	if len(dh.PC.credentialSourceRefs) > 0 {
		sc := &servicecredential.ServiceCredential{SourceID: dh.PC.Source.ID, TenantID: dh.PC.Tenant.ID}
		if err := dh.PC.servicecredentialrepo.DeleteUnwanted(ctx, dh.PC.logger, sc, dh.PC.credentialSourceRefs); err != nil {
			dh.PC.logger.Errorf("Error deleting Service Credentials %v", err)
			return err
		}
	}

	if len(dh.PC.credentialTypeSourceRefs) > 0 {
		sct := &servicecredentialtype.ServiceCredentialType{SourceID: dh.PC.Source.ID, TenantID: dh.PC.Tenant.ID}
		if err := dh.PC.servicecredentialtyperepo.DeleteUnwanted(ctx, dh.PC.logger, sct, dh.PC.credentialTypeSourceRefs); err != nil {
			dh.PC.logger.Errorf("Error deleting Service credential types %v", err)
			return err
		}
	}
	return nil
}
