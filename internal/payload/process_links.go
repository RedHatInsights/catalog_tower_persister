package payload

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredential"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceoffering"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceofferingnode"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceplan"
	"gorm.io/gorm"
)

//ProcessLinks builds the links between different objects
func (bol *BillOfLading) ProcessLinks(ctx context.Context, dbTransaction *gorm.DB) error {
	err := bol.updateInventoryLink(ctx, dbTransaction)
	if err != nil {
		return err
	}
	err = bol.updateCredentialTypeLink(ctx, dbTransaction)
	if err != nil {
		return err
	}
	err = bol.updateSurveyLink(ctx, dbTransaction)
	if err != nil {
		return err
	}
	return bol.updateServiceNodeLink(ctx, dbTransaction)
}

func (bol *BillOfLading) updateServiceNodeLink(ctx context.Context, dbTransaction *gorm.DB) error {
	for _, w := range bol.workflowNodes {
		var son serviceofferingnode.ServiceOfferingNode
		if result := dbTransaction.Where("source_ref= ? AND tenant_id = ? AND source_id = ?", w.SourceRef, bol.tenant.ID, bol.source.ID).First(&son); result.Error != nil {
			return fmt.Errorf("Error finding service offering node  %s : %v", w.SourceRef, result.Error.Error())
		}

		var so serviceoffering.ServiceOffering
		if result := dbTransaction.Where("source_ref= ? AND tenant_id = ? AND source_id = ?", w.ServiceOfferingSourceRef, bol.tenant.ID, bol.source.ID).First(&so); result.Error != nil {
			return fmt.Errorf("Error finding service offering %s : %v", w.ServiceOfferingSourceRef, result.Error.Error())
		}
		var rso serviceoffering.ServiceOffering
		if result := dbTransaction.Where("source_ref= ? AND tenant_id = ? AND source_id = ?", w.RootServiceOfferingSourceRef, bol.tenant.ID, bol.source.ID).First(&rso); result.Error != nil {
			return fmt.Errorf("Error finding root service offering %s : %v", w.RootServiceOfferingSourceRef, result.Error.Error())
		}
		son.ServiceOfferingID = sql.NullInt64{Int64: so.ID, Valid: true}
		son.RootServiceOfferingID = sql.NullInt64{Int64: rso.ID, Valid: true}
		if result := dbTransaction.Save(&son); result.Error != nil {
			return fmt.Errorf("Error saving service offering node  %s : %v", w.SourceRef, result.Error.Error())
		}
	}
	return nil
}

func (bol *BillOfLading) updateSurveyLink(ctx context.Context, dbTransaction *gorm.DB) error {
	for _, v := range bol.jobTemplateSurvey {
		err := bol.setSurvey(ctx, dbTransaction, v)
		if err != nil {
			return err
		}
	}
	for _, v := range bol.workflowJobTemplateSurvey {
		err := bol.setSurvey(ctx, dbTransaction, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bol *BillOfLading) setSurvey(ctx context.Context, dbTransaction *gorm.DB, sourceRef string) error {
	var sp serviceplan.ServicePlan
	var so serviceoffering.ServiceOffering

	if result := dbTransaction.Where("source_ref= ? AND tenant_id = ? AND source_id = ?", sourceRef, bol.tenant.ID, bol.source.ID).First(&sp); result.Error != nil {

		return fmt.Errorf("Error finding service plan %s : %v", sourceRef, result.Error.Error())
	}

	if result := dbTransaction.Where("source_ref= ? AND tenant_id = ? AND source_id = ?", sourceRef, bol.tenant.ID, bol.source.ID).First(&so); result.Error != nil {
		return fmt.Errorf("Error finding service offering %s : %v", sourceRef, result.Error.Error())
	}

	sp.ServiceOfferingID = sql.NullInt64{Int64: so.ID, Valid: true}
	if result := dbTransaction.Save(&sp); result.Error != nil {
		return fmt.Errorf("Error saving service plan %s : %v", sourceRef, result.Error.Error())
	}
	return nil
}

func (bol *BillOfLading) updateInventoryLink(ctx context.Context, dbTransaction *gorm.DB) error {
	for k, v := range bol.inventoryMap {
		var si serviceinventory.ServiceInventory
		if result := dbTransaction.Where("source_ref= ? AND tenant_id = ? AND source_id = ?", k, bol.tenant.ID, bol.source.ID).First(&si); result.Error != nil {

			return fmt.Errorf("Error finding service inventory by src ref %v : %v", k, result.Error.Error())
		}
		for _, id := range v {
			var so serviceoffering.ServiceOffering
			if result := dbTransaction.Where("ID = ?", id).First(&so); result.Error != nil {
				return fmt.Errorf("Error finding service offering %v : %v", id, result.Error.Error())
			}
			so.ServiceInventoryID = sql.NullInt64{Int64: si.ID, Valid: true}
			if result := dbTransaction.Save(&so); result.Error != nil {
				return fmt.Errorf("Error saving service offering %v : %v", id, result.Error.Error())
			}
		}
	}
	return nil
}

func (bol *BillOfLading) updateCredentialTypeLink(ctx context.Context, dbTransaction *gorm.DB) error {
	for k, v := range bol.serviceCredentialToCredentialTypeMap {
		var sct servicecredentialtype.ServiceCredentialType
		if result := dbTransaction.Where("source_ref= ? AND tenant_id = ? AND source_id = ?", k, bol.tenant.ID, bol.source.ID).First(&sct); result.Error != nil {
			return fmt.Errorf("Error finding service cerdential type %v : %v", k, result.Error.Error())
		}
		for _, id := range v {
			var sc servicecredential.ServiceCredential
			if result := dbTransaction.Where("ID = ?", id).First(&sc); result.Error != nil {
				return fmt.Errorf("Error finding service credential %v : %v", id, result.Error.Error())
			}
			sc.ServiceCredentialTypeID = sql.NullInt64{Int64: sct.ID, Valid: true}
			if result := dbTransaction.Save(&sc); result.Error != nil {
				return fmt.Errorf("Error saving service credential %v : %v", id, result.Error.Error())
			}
		}
	}
	return nil
}
