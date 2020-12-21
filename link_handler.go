package main

import (
	"database/sql"
	"fmt"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/base"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredential"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceoffering"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceofferingnode"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceplan"
)

type LinkHandler struct {
	PC *PageContext
}

func (lh *LinkHandler) Process() error {
	lh.updateInventoryLink()
	lh.updateCredentialTypeLink()
	lh.updateSurveyLink()
	lh.updateServiceNodeLink()
	return nil
}

func (lh *LinkHandler) updateServiceNodeLink() error {
	for _, w := range lh.PC.WorkflowNodes {
		son := serviceofferingnode.ServiceOfferingNode{Tower: base.Tower{SourceRef: w.SourceRef}, TenantID: lh.PC.Tenant.ID, SourceID: lh.PC.Source.ID}
		if result := lh.PC.dbTransaction.Where(&son).First(&son); result.Error != nil {
			return fmt.Errorf("Error finding service offering node  %s : %v"+w.SourceRef, result.Error.Error())
		}

		so := serviceoffering.ServiceOffering{Tower: base.Tower{SourceRef: w.ServiceOfferingSourceRef}, TenantID: lh.PC.Tenant.ID, SourceID: lh.PC.Source.ID}
		if result := lh.PC.dbTransaction.Where(&so).First(&so); result.Error != nil {
			return fmt.Errorf("Error finding service offering %s : %v"+w.ServiceOfferingSourceRef, result.Error.Error())
		}
		rso := serviceoffering.ServiceOffering{Tower: base.Tower{SourceRef: w.RootServiceOfferingSourceRef}, TenantID: lh.PC.Tenant.ID, SourceID: lh.PC.Source.ID}
		if result := lh.PC.dbTransaction.Where(&rso).First(&rso); result.Error != nil {
			return fmt.Errorf("Error finding root service offering %s : %v"+w.RootServiceOfferingSourceRef, result.Error.Error())
		}
		son.ServiceOfferingID = sql.NullInt64{Int64: so.ID, Valid: true}
		son.RootServiceOfferingID = sql.NullInt64{Int64: rso.ID, Valid: true}
		if result := lh.PC.dbTransaction.Save(&son); result.Error != nil {
			return fmt.Errorf("Error saving service offering node  %s : %v"+w.SourceRef, result.Error.Error())
		}
	}
	return nil
}

func (lh *LinkHandler) updateSurveyLink() error {
	for _, v := range lh.PC.JobTemplateSurvey {
		lh.setSurvey(v)
	}
	for _, v := range lh.PC.WorkflowJobTemplateSurvey {
		lh.setSurvey(v)
	}
	return nil
}

func (lh *LinkHandler) setSurvey(sourceRef string) error {
	sp := serviceplan.ServicePlan{Tower: base.Tower{SourceRef: sourceRef}, TenantID: lh.PC.Tenant.ID, SourceID: lh.PC.Source.ID}
	so := serviceoffering.ServiceOffering{Tower: base.Tower{SourceRef: sourceRef}, TenantID: lh.PC.Tenant.ID, SourceID: lh.PC.Source.ID}
	if result := lh.PC.dbTransaction.Where(&sp).First(&sp); result.Error != nil {
		return fmt.Errorf("Error finding service plan %s : %v"+sourceRef, result.Error.Error())
	}
	if result := lh.PC.dbTransaction.Where(&so).First(&so); result.Error != nil {
		return fmt.Errorf("Error finding service offering %s : %v"+sourceRef, result.Error.Error())
	}
	sp.ServiceOfferingID = sql.NullInt64{Int64: so.ID, Valid: true}
	if result := lh.PC.dbTransaction.Save(&sp); result.Error != nil {
		return fmt.Errorf("Error saving service plan %s : %v"+sourceRef, result.Error.Error())
	}
	return nil
}

func (lh *LinkHandler) updateInventoryLink() error {
	for k, v := range lh.PC.InventoryMap {
		var si serviceinventory.ServiceInventory
		lh.PC.dbTransaction.Where("source_ref= ? AND tenant_id = ? AND source_id = ?", k, lh.PC.Tenant.ID, lh.PC.Source.ID).First(&si)
		for _, id := range v {
			var so serviceoffering.ServiceOffering
			lh.PC.dbTransaction.Where("ID = ?", id).First(&so)
			so.ServiceInventoryID = sql.NullInt64{Int64: si.ID, Valid: true}
			lh.PC.dbTransaction.Save(&so)
		}
	}
	return nil
}

func (lh *LinkHandler) updateCredentialTypeLink() error {
	for k, v := range lh.PC.ServiceCredentialToCredentialTypeMap {
		var sct servicecredentialtype.ServiceCredentialType
		lh.PC.dbTransaction.Where("source_ref= ? AND tenant_id = ? AND source_id = ?", k, lh.PC.Tenant.ID, lh.PC.Source.ID).First(&sct)
		for _, id := range v {
			var sc servicecredential.ServiceCredential
			lh.PC.dbTransaction.Where("ID = ?", id).First(&sc)
			sc.ServiceCredentialTypeID = sql.NullInt64{Int64: sct.ID, Valid: true}
			lh.PC.dbTransaction.Save(&sc)
		}
	}
	return nil
}
