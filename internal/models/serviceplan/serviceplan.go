package serviceplan

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/base"
	"github.com/sirupsen/logrus"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// ServicePlan maps the SurveySpec from Ansible Tower
type ServicePlan struct {
	base.Base
	base.Tower
	Name              string
	Description       string
	Extra             datatypes.JSON
	CreateJSONSchema  datatypes.JSON
	UpdateJSONSchema  datatypes.JSON
	TenantID          int64
	SourceID          int64
	ServiceOfferingID sql.NullInt64 `gorm:"default:null"`
}

//DDFConverter interface to convert Tower SPEC to DDF format
type DDFConverter interface {
	Convert(ctx context.Context, logger *logrus.Entry, r io.Reader) ([]byte, error)
}

// Repository interface supports deleted unwanted objects and creating or updating object
type Repository interface {
	Delete(ctx context.Context, logger *logrus.Entry, sp *ServicePlan) error
	CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sp *ServicePlan, converter DDFConverter, attrs map[string]interface{}, r io.Reader) error
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

func (gr *gormRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sp *ServicePlan, converter DDFConverter, attrs map[string]interface{}, r io.Reader) error {
	err := sp.makeObject(ctx, logger, converter, attrs, r)
	if err != nil {
		logger.Infof("Error creating a new service plan object %v", err)
		return err
	}
	var instance ServicePlan
	err = gr.db.Where(&ServicePlan{SourceID: sp.SourceID, Tower: base.Tower{SourceRef: sp.SourceRef}}).First(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			logger.Infof("Creating a new Survey Spec %s", sp.SourceRef)
			if result := gr.db.Create(sp); result.Error != nil {
				return fmt.Errorf("Error creating survey spec: %v", result.Error.Error())
			}
		} else {
			logger.Infof("Error locating Survey Spec %s %v", sp.SourceRef, err)
			return err
		}
		gr.creates++
	} else {
		logger.Infof("Survey Spec %s exists in DB with ID %d", sp.SourceRef, instance.ID)
		sp.ID = instance.ID // Get the Existing ID for the object
		instance.CreateJSONSchema = sp.CreateJSONSchema
		instance.Description = sp.Description
		instance.Name = sp.Name

		logger.Infof("Saving Survey Spec  source_ref %s", sp.SourceRef)
		err := gr.db.Save(&instance).Error
		if err != nil {
			logger.Errorf("Error Updating Service Plan  source_ref %s", sp.SourceRef)
			return err
		}
		gr.updates++
	}
	return nil
}

func (gr *gormRepository) Delete(ctx context.Context, logger *logrus.Entry, sp *ServicePlan) error {
	err := gr.db.Model(&ServicePlan{}).Where("source_ref = ? AND source_id = ?", sp.SourceRef, sp.SourceID).Delete(&ServicePlan{}).Error
	if err == nil {
		gr.deletes++
	}
	return err
}

func (sp *ServicePlan) validateAttributes(attrs map[string]interface{}) error {
	requiredAttrs := []string{"name",
		"description"}
	for _, name := range requiredAttrs {
		if _, ok := attrs[name]; !ok {
			return errors.New("Missing Required Attribute " + name)
		}
	}
	return nil
}

func (sp *ServicePlan) makeObject(ctx context.Context, logger *logrus.Entry, converter DDFConverter, attrs map[string]interface{}, r io.Reader) error {
	err := sp.validateAttributes(attrs)
	if err != nil {
		return err
	}
	spec, err := converter.Convert(ctx, logger, r)
	if err != nil {
		logger.Errorf("Error converting service plan %v", err)
		return err
	}
	sp.CreateJSONSchema = datatypes.JSON(spec)
	sp.Description = attrs["description"].(string)
	sp.Name = attrs["name"].(string)
	sp.SourceRef = attrs["id"].(json.Number).String()
	return nil
}
