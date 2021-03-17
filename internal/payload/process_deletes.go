package payload

import (
	"context"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredential"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceoffering"
)

//ProcessDeletes deletes unwanted objects
func (bol *BillOfLading) ProcessDeletes(ctx context.Context) error {
	if len(bol.jobTemplateSourceRefs) > 0 {
		so := &serviceoffering.ServiceOffering{SourceID: bol.source.ID, TenantID: bol.tenant.ID}
		if err := bol.repos.serviceofferingrepo.DeleteUnwanted(ctx, bol.logger, so, bol.jobTemplateSourceRefs, bol.repos.serviceplanrepo); err != nil {
			bol.logger.Errorf("Error deleting Service Offering %v", err)
			return err
		}
	}

	if len(bol.inventorySourceRefs) > 0 {
		si := &serviceinventory.ServiceInventory{SourceID: bol.source.ID, TenantID: bol.tenant.ID}
		if err := bol.repos.serviceinventoryrepo.DeleteUnwanted(ctx, bol.logger, si, bol.inventorySourceRefs); err != nil {
			bol.logger.Errorf("Error deleting Service Inventories %v", err)
			return err
		}
	}

	if len(bol.credentialSourceRefs) > 0 {
		sc := &servicecredential.ServiceCredential{SourceID: bol.source.ID, TenantID: bol.tenant.ID}
		if err := bol.repos.servicecredentialrepo.DeleteUnwanted(ctx, bol.logger, sc, bol.credentialSourceRefs); err != nil {
			bol.logger.Errorf("Error deleting Service Credentials %v", err)
			return err
		}
	}

	if len(bol.credentialTypeSourceRefs) > 0 {
		sct := &servicecredentialtype.ServiceCredentialType{SourceID: bol.source.ID, TenantID: bol.tenant.ID}
		if err := bol.repos.servicecredentialtyperepo.DeleteUnwanted(ctx, bol.logger, sct, bol.credentialTypeSourceRefs); err != nil {
			bol.logger.Errorf("Error deleting Service credential types %v", err)
			return err
		}
	}
	return nil
}
