package servicecredential

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/base"
	"github.com/sirupsen/logrus"

	"gorm.io/gorm"
)

// ServiceCredential model object
type ServiceCredential struct {
	base.Base
	base.Tower
	Name                           string
	TypeName                       string
	Description                    string
	TenantID                       int64
	SourceID                       int64
	ServiceCredentialTypeID        sql.NullInt64 `gorm:"default:null"`
	ServiceCredentialTypeSourceRef string        `gorm:"-"`
}

// Repository interface supports deleted unwanted objects and creating or updating object
type Repository interface {
	DeleteUnwanted(ctx context.Context, logger *logrus.Entry, sc *ServiceCredential, keepSourceRefs []string) error
	CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sc *ServiceCredential, attrs map[string]interface{}) error
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

// CreateOrUpdate a ServiceCredential Object in the Database
func (gr *gormRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sc *ServiceCredential, attrs map[string]interface{}) error {
	err := sc.makeObject(attrs)
	if err != nil {
		logger.Errorf("Error creating a new service credential object %v", err)
		return err
	}

	var instance ServiceCredential
	err = gr.db.Where(&ServiceCredential{SourceID: sc.SourceID, Tower: base.Tower{SourceRef: sc.SourceRef}}).First(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			logger.Infof("Creating a new Credential %s", sc.SourceRef)
			if result := gr.db.Create(sc); result.Error != nil {
				return fmt.Errorf("Error creating service credential : %v", result.Error.Error())
			}
			gr.creates++
		} else {
			logger.Errorf("Error locating Credential %s %v", sc.SourceRef, err)
			return err
		}
	} else {
		logger.Infof("Service Credential %s exists in DB with ID %d", sc.SourceRef, instance.ID)
		sc.ID = instance.ID // Get the Existing ID for the object
		instance.Description = sc.Description
		instance.Name = sc.Name
		instance.ServiceCredentialTypeSourceRef = sc.ServiceCredentialTypeSourceRef

		if instance.SourceUpdatedAt != sc.SourceUpdatedAt {
			instance.SourceUpdatedAt = sc.SourceUpdatedAt
			logger.Infof("Saving Service Credential source_ref %s", sc.SourceRef)
			err := gr.db.Save(&instance).Error
			if err != nil {
				logger.Errorf("Error Updating Service Credential  source_ref %s", sc.SourceRef)
				return err
			}
			gr.updates++
		}
	}
	return nil
}

// DeleteUnwanted deletes any objects not listed in the keepSourceRefs
// This is used to delete ServiceCredentials that exist in our database but have been
// deleted from the Ansible Tower
func (gr *gormRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, sc *ServiceCredential, keepSourceRefs []string) error {
	results, err := sc.getDeleteIDs(ctx, logger, gr.db, keepSourceRefs)
	if err != nil {
		logger.Errorf("Error getting Delete IDs for service credentials %v", err)
		return err
	}
	for _, res := range results {
		logger.Infof("Attempting to delete ServiceCredential with ID %d Source ref %s", res.ID, res.SourceRef)
		result := gr.db.Delete(&ServiceCredential{SourceID: sc.SourceID, TenantID: sc.TenantID, Tower: base.Tower{SourceRef: res.SourceRef}}, res.ID)
		if result.Error != nil {
			logger.Errorf("Error deleting Service Credential %d %s %v", res.ID, res.SourceRef, result.Error)
			return result.Error
		}
		gr.deletes++
	}
	return nil
}

func (sc *ServiceCredential) validateAttributes(attrs map[string]interface{}) error {
	requiredAttrs := []string{"created",
		"modified",
		"name",
		"id",
		"description",
		"credential_type"}
	for _, name := range requiredAttrs {
		if _, ok := attrs[name]; !ok {
			return errors.New("Missing Required Attribute " + name)
		}
	}
	return nil
}

func (sc *ServiceCredential) makeObject(attrs map[string]interface{}) error {
	err := sc.validateAttributes(attrs)
	if err != nil {
		return err
	}

	sc.SourceCreatedAt, err = base.TowerTime(attrs["created"].(string))
	if err != nil {
		return err
	}
	sc.SourceUpdatedAt, err = base.TowerTime(attrs["modified"].(string))
	if err != nil {
		return err
	}
	sc.Description = attrs["description"].(string)
	sc.Name = attrs["name"].(string)
	sc.SourceRef = attrs["id"].(json.Number).String()
	sc.ServiceCredentialTypeSourceRef = attrs["credential_type"].(json.Number).String()
	return nil
}

func (sc *ServiceCredential) getDeleteIDs(ctx context.Context, logger *logrus.Entry, tx *gorm.DB, keepSourceRefs []string) ([]base.ResultIDRef, error) {
	var result []base.ResultIDRef
	var deleteResultIDRef []base.ResultIDRef
	sort.Strings(keepSourceRefs)
	length := len(keepSourceRefs)
	if err := tx.Table("service_credentials").Select("id, source_ref").Where("source_id = ? AND archived_at IS NULL", sc.SourceID).Scan(&result).Error; err != nil {
		logger.Errorf("Error fetching ServiceCredential %v", err)
		return deleteResultIDRef, err
	}
	for _, res := range result {
		if !base.SourceRefExists(res.SourceRef, keepSourceRefs, length) {
			deleteResultIDRef = append(deleteResultIDRef, res)
		}
	}
	return deleteResultIDRef, nil
}
