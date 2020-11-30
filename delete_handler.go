package main

import (
	"github.com/mkanoor/catalog_tower_persister/internal/models/servicecredential"
	"github.com/mkanoor/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/mkanoor/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/mkanoor/catalog_tower_persister/internal/models/serviceoffering"
)

type DeleteHandler struct {
	PC *PageContext
}

func (dh *DeleteHandler) Process() error {
	if len(dh.PC.jobTemplateSourceRefs) > 0 {
		so := &serviceoffering.ServiceOffering{Source: *dh.PC.Source, Tenant: *dh.PC.Tenant}
		if err := so.DeleteOldServiceOfferings(dh.PC.dbTransaction, dh.PC.jobTemplateSourceRefs); err != nil {
			dh.PC.glog.Errorf("Error deleting Service Offering %v", err)
			return err
		}
	}

	if len(dh.PC.inventorySourceRefs) > 0 {
		si := &serviceinventory.ServiceInventory{Source: *dh.PC.Source, Tenant: *dh.PC.Tenant}
		if err := si.DeleteOldServiceInventories(dh.PC.dbTransaction, dh.PC.inventorySourceRefs); err != nil {
			dh.PC.glog.Errorf("Error deleting Service Inventories %v", err)
			return err
		}
	}

	if len(dh.PC.credentialSourceRefs) > 0 {
		sc := &servicecredential.ServiceCredential{Source: *dh.PC.Source, Tenant: *dh.PC.Tenant}
		if err := sc.DeleteOldServiceCredentials(dh.PC.dbTransaction, dh.PC.credentialSourceRefs); err != nil {
			dh.PC.glog.Errorf("Error deleting Service Credentials %v", err)
			return err
		}
	}

	if len(dh.PC.credentialTypeSourceRefs) > 0 {
		sct := &servicecredentialtype.ServiceCredentialType{Source: *dh.PC.Source, Tenant: *dh.PC.Tenant}
		if err := sct.DeleteOldServiceCredentialTypes(dh.PC.dbTransaction, dh.PC.credentialTypeSourceRefs); err != nil {
			dh.PC.glog.Errorf("Error deleting Service credential types %v", err)
			return err
		}
	}
	return nil
}
