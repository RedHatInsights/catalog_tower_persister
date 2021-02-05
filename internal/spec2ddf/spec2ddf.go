package spec2ddf

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/sirupsen/logrus"
)

type ddfField struct {
	Name         string                   `json:"name"`
	Label        string                   `json:"label"`
	Component    string                   `json:"component"`
	HelperText   string                   `json:"helperText,omitempty"`
	InitialValue interface{}              `json:"initialValue"`
	IsRequired   bool                     `json:"isRequired"`
	Validate     []map[string]interface{} `json:"validate,omitempty"`
	DataType     string                   `json:"dataType,omitempty"`
	Options      interface{}              `json:"options,omitempty"`
	Type         string                   `json:"type,omitempty"`
	Multi        bool                     `json:"multi,omitempty"`
}

type field struct {
	QuestionName        string      `json:"question_name"`
	QuestionDescription string      `json:"question_description"`
	Required            bool        `json:"required"`
	Type                string      `json:"type"`
	Variable            string      `json:"variable"`
	Min                 interface{} `json:"min"`
	Max                 interface{} `json:"max"`
	Default             interface{} `json:"default"`
	Choices             interface{} `json:"choices"`
}

type surveySpec struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Fields      []field `json:"spec"`
}

type ddfSchema struct {
	Title       string     `json:"title"`
	Description string     `json:"description"`
	Fields      []ddfField `json:"fields"`
}

type ddfSpec struct {
	Schema     ddfSchema `json:"schema"`
	SchemaType string    `json:"schemaType"`
}

// Converter used to convert SPEC from Ansible tower to DDF
type Converter struct{}

// Convert will transform a Tower Survey Spec to the DDF format
func (sc *Converter) Convert(ctx context.Context, logger *logrus.Entry, r io.Reader) ([]byte, error) {
	var ss surveySpec
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	err := decoder.Decode(&ss)
	if err != nil {
		return nil, err
	}

	var ddfs ddfSpec
	var ddfSchema ddfSchema
	ddfs.SchemaType = "default"

	for _, f := range ss.Fields {
		cf, err := getDDFField(&f)
		if err != nil {
			return nil, err
		}
		ddfSchema.Fields = append(ddfSchema.Fields, *cf)
	}
	ddfs.Schema = ddfSchema
	ddfSchema.Title = ss.Name
	ddfSchema.Description = ss.Description

	return json.Marshal(&ddfs)
}

func getDDFField(f *field) (*ddfField, error) {
	ddff := ddfField{Label: f.QuestionName,
		Name:         f.Variable,
		InitialValue: f.Default,
		HelperText:   f.QuestionDescription,
		IsRequired:   f.Required}
	ddff.Validate = getValidateArray(f)
	opts := getOptions(f)
	if opts != nil {
		ddff.Options = opts
	}
	if f.Type == "multiplechoice" {
		ddff.Component = "select-field"
	} else if f.Type == "multiselect" {
		ddff.Component = "select-field"
		ddff.Multi = true
	} else if f.Type == "text" {
		ddff.Component = "text-field"
	} else if f.Type == "integer" {
		ddff.Type = "number"
		ddff.DataType = "integer"
		ddff.Component = "text-field"
	} else if f.Type == "float" {
		ddff.Type = "number"
		ddff.DataType = "float"
		ddff.Component = "text-field"
	} else if f.Type == "password" {
		ddff.Type = "password"
		ddff.Component = "text-field"
	} else if f.Type == "textarea" {
		ddff.Component = "textarea-field"
	} else {
		return nil, fmt.Errorf("Unsupported field type %s", f.Type)
	}

	return &ddff, nil
}

func getValidateArray(f *field) []map[string]interface{} {

	var result []map[string]interface{}
	if f.Required {
		result = append(result, map[string]interface{}{"type": "required-validator"})
	}

	switch f.Min.(type) {
	case json.Number:
		if f.Type == "text" || f.Type == "password" || f.Type == "textarea" {
			result = append(result, map[string]interface{}{"type": "min-length-validator",
				"threshold": f.Min.(json.Number)})
		} else if f.Type == "integer" || f.Type == "float" {
			result = append(result, map[string]interface{}{"type": "min-number-value",
				"value": f.Min.(json.Number)})
		}
	}

	switch f.Max.(type) {
	case json.Number:
		if f.Type == "text" || f.Type == "password" || f.Type == "textarea" {
			result = append(result, map[string]interface{}{"type": "max-length-validator",
				"threshold": f.Max.(json.Number)})
		} else if f.Type == "integer" || f.Type == "float" {
			result = append(result, map[string]interface{}{"type": "max-number-value",
				"value": f.Max.(json.Number)})
		}
	}

	return result
}

func getOptions(f *field) []map[string]interface{} {
	var result []map[string]interface{}
	var values []string
	switch f.Choices.(type) {
	case string:
		value := f.Choices.(string)
		if value == "" {
			return nil
		}
		values = strings.Split(value, "\n")
	case []string:
		values = f.Choices.([]string)
	default:
		return nil
	}

	if len(values) == 0 {
		return nil
	}
	for _, v := range values {
		result = append(result, map[string]interface{}{"label": v, "value": v})
	}
	return result
}
