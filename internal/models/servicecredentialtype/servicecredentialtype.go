package servicecredentialtype

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/base"
	"github.com/sirupsen/logrus"

	"gorm.io/gorm"
)

// ServiceCredentialType maps a Credential Type from Ansible Tower
type ServiceCredentialType struct {
	base.Base
	base.Tower
	Name        string
	Description string
	Kind        string
	Namespace   string
	TenantID    int64
	SourceID    int64
}

// Repository interface supports deleted unwanted objects and creating or updating object
type Repository interface {
	DeleteUnwanted(ctx context.Context, logger *logrus.Entry, sct *ServiceCredentialType, keepSourceRefs []string) error
	CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sct *ServiceCredentialType, attrs map[string]interface{}) error
	Stats() map[string]int
}

// gormRepository struct stores the DB handle and counters
type gormRepository struct {
	db      *gorm.DB
	updates int
	creates int
	deletes int
}

// NewGORMRepository creates a new repository object
func NewGORMRepository(db *gorm.DB) Repository {
	return &gormRepository{db: db}
}

// Stats returns a map with the number of adds/updates/deletes
func (gr *gormRepository) Stats() map[string]int {
	return map[string]int{"adds": gr.creates, "updates": gr.updates, "deletes": gr.deletes}
}

// CreateOrUpdate a ServiceCredentialType Object in the Database
func (gr *gormRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sct *ServiceCredentialType, attrs map[string]interface{}) error {
	err := sct.makeObject(attrs)
	if err != nil {
		logger.Infof("Error creating a new credential type object %v", err)
		return err
	}

	var instance ServiceCredentialType

	err = gr.db.Where(&ServiceCredentialType{SourceID: sct.SourceID, Tower: base.Tower{SourceRef: sct.SourceRef}}).First(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			logger.Infof("Creating a new Credential Type %s", sct.SourceRef)
			if result := gr.db.Create(sct); result.Error != nil {
				return fmt.Errorf("Error creating service credential type : %v", result.Error.Error())
			}
			gr.creates++
		} else {
			logger.Infof("Error locating Credential Type %s %v", sct.SourceRef, err)
			return err
		}

	} else {
		logger.Infof("Service Credential Type %s exists in DB with ID %d", sct.SourceRef, instance.ID)
		sct.ID = instance.ID // Get the Existing ID for the object
		instance.Description = sct.Description
		instance.Name = sct.Name
		instance.Namespace = sct.Namespace
		instance.Kind = sct.Kind

		logger.Infof("Saving Service Credential Type source_ref %s", sct.SourceRef)
		err := gr.db.Save(&instance).Error
		if err != nil {
			logger.Errorf("Error Updating Service Credential Type source_ref %s", sct.SourceRef)
			return err
		}
		gr.updates++
	}
	return nil
}

// DeleteUnwanted deletes any objects not listed in the keepSourceRefs
// This is used to delete ServiceCredentialTypes that exist in our database but have been
// deleted from the Ansible Tower
func (gr *gormRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, sct *ServiceCredentialType, keepSourceRefs []string) error {
	results, err := sct.getDeleteIDs(ctx, logger, gr.db, keepSourceRefs)
	if err != nil {
		logger.Errorf("Error getting Delete IDs for service credential types %v", err)
		return err
	}
	for _, res := range results {
		logger.Infof("Attempting to delete ServiceCredentialType with ID %d Source ref %s", res.ID, res.SourceRef)
		result := gr.db.Delete(&ServiceCredentialType{SourceID: sct.SourceID, TenantID: sct.TenantID, Tower: base.Tower{SourceRef: res.SourceRef}}, res.ID)
		if result.Error != nil {
			logger.Errorf("Error deleting Service CredentialType %d %s %v", res.ID, res.SourceRef, result.Error)
			return result.Error
		}
		gr.deletes++
	}
	return nil
}
func (sct *ServiceCredentialType) validateAttributes(attrs map[string]interface{}) error {
	requiredAttrs := []string{"kind",
		"namespace",
		"created",
		"modified",
		"name",
		"id",
		"description"}
	for _, name := range requiredAttrs {
		if _, ok := attrs[name]; !ok {
			return errors.New("Missing Required Attribute " + name)
		}
	}
	return nil
}

func (sct *ServiceCredentialType) makeObject(attrs map[string]interface{}) error {
	err := sct.validateAttributes(attrs)
	if err != nil {
		return err
	}

	sct.SourceCreatedAt, err = base.TowerTime(attrs["created"].(string))
	if err != nil {
		return err
	}
	/*sct.SourceUpdatedAt, err = towerTime(attrs["modified"].(string))
	if err != nil {
		return err
	}*/
	sct.Description = attrs["description"].(string)
	sct.Kind = attrs["kind"].(string)
	sct.Namespace = attrs["namespace"].(string)
	sct.Name = attrs["name"].(string)
	sct.SourceRef = attrs["id"].(json.Number).String()
	return nil
}

func (sct *ServiceCredentialType) getDeleteIDs(ctx context.Context, logger *logrus.Entry, tx *gorm.DB, keepSourceRefs []string) ([]base.ResultIDRef, error) {
	var result []base.ResultIDRef
	var deleteResultIDRef []base.ResultIDRef
	sort.Strings(keepSourceRefs)
	length := len(keepSourceRefs)
	if err := tx.Table("service_credential_types").Select("id, source_ref").Where("source_id = ? AND archived_at IS NULL", sct.SourceID).Scan(&result).Error; err != nil {
		logger.Errorf("Error fetching ServiceCredentialType %v", err)
		return deleteResultIDRef, err
	}
	for _, res := range result {
		if !base.SourceRefExists(res.SourceRef, keepSourceRefs, length) {
			deleteResultIDRef = append(deleteResultIDRef, res)
		}
	}
	return deleteResultIDRef, nil
}
