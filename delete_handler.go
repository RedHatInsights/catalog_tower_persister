package main

import (
	"context"

	"github.com/mkanoor/catalog_tower_persister/internal/models/servicecredential"
	"github.com/mkanoor/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/mkanoor/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/mkanoor/catalog_tower_persister/internal/models/serviceoffering"
)

type DeleteHandler struct {
	PC *PageContext
}

func (dh *DeleteHandler) Process(ctx context.Context) error {
	if len(dh.PC.jobTemplateSourceRefs) > 0 {
		so := &serviceoffering.ServiceOffering{SourceID: dh.PC.Source.ID, TenantID: dh.PC.Tenant.ID}
		if err := so.DeleteOldServiceOfferings(ctx, dh.PC.dbTransaction, dh.PC.jobTemplateSourceRefs); err != nil {
			dh.PC.glog.Errorf("Error deleting Service Offering %v", err)
			return err
		}
	}

	if len(dh.PC.inventorySourceRefs) > 0 {
		si := &serviceinventory.ServiceInventory{SourceID: dh.PC.Source.ID, TenantID: dh.PC.Tenant.ID}
		if err := si.DeleteOldServiceInventories(ctx, dh.PC.dbTransaction, dh.PC.inventorySourceRefs); err != nil {
			dh.PC.glog.Errorf("Error deleting Service Inventories %v", err)
			return err
		}
	}

	if len(dh.PC.credentialSourceRefs) > 0 {
		sc := &servicecredential.ServiceCredential{SourceID: dh.PC.Source.ID, TenantID: dh.PC.Tenant.ID}
		if err := dh.PC.servicecredentialhandler.Delete(ctx, dh.PC.dbTransaction, sc, dh.PC.credentialSourceRefs); err != nil {
			dh.PC.glog.Errorf("Error deleting Service Credentials %v", err)
			return err
		}
	}

	if len(dh.PC.credentialTypeSourceRefs) > 0 {
		sct := &servicecredentialtype.ServiceCredentialType{SourceID: dh.PC.Source.ID, TenantID: dh.PC.Tenant.ID}
		if err := sct.DeleteOldServiceCredentialTypes(ctx, dh.PC.dbTransaction, dh.PC.credentialTypeSourceRefs); err != nil {
			dh.PC.glog.Errorf("Error deleting Service credential types %v", err)
			return err
		}
	}
	return nil
}
