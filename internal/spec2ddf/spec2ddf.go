package spec2ddf

import (
	"encoding/json"
	"io"
	"strings"
)

type DDFField struct {
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

type Field struct {
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

type SurveySpec struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Fields      []Field `json:"spec"`
}

type DDFSchema struct {
	Title       string     `json:"title"`
	Description string     `json:"description"`
	Fields      []DDFField `json:"fields"`
}

type DDFSpec struct {
	Schema     DDFSchema `json:"schema"`
	SchemaType string    `json:"schemaType"`
}

func Convert(r io.Reader) ([]byte, error) {
	var ss SurveySpec
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	err := decoder.Decode(&ss)
	if err != nil {
		return nil, err
	}

	var ddfs DDFSpec
	var ddfSchema DDFSchema
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

func getDDFField(field *Field) (*DDFField, error) {
	ddff := DDFField{Label: field.QuestionName,
		Name:         field.Variable,
		InitialValue: field.Default,
		HelperText:   field.QuestionDescription,
		IsRequired:   field.Required}
	ddff.Validate = getValidateArray(field)
	opts := getOptions(field)
	if opts != nil {
		ddff.Options = opts
	}
	if field.Type == "multiplechoice" {
		ddff.Component = "select-field"
	} else if field.Type == "multiselect" {
		ddff.Component = "select-field"
		ddff.Multi = true
	} else if field.Type == "text" {
		ddff.Component = "text-field"
	} else if field.Type == "integer" {
		ddff.Type = "number"
		ddff.DataType = "integer"
	} else if field.Type == "float" {
		ddff.Type = "number"
		ddff.DataType = "float"
	} else if field.Type == "password" {
		ddff.Type = "password"
		ddff.Component = "text-field"
	} else if field.Type == "textarea" {
		ddff.Component = "textarea-field"
	}

	return &ddff, nil
}

func getValidateArray(field *Field) []map[string]interface{} {

	var result []map[string]interface{}
	if field.Required {
		result = append(result, map[string]interface{}{"type": "required-validator"})
	}

	switch field.Min.(type) {
	case json.Number:
		if field.Type == "text" || field.Type == "password" || field.Type == "textarea" {
			result = append(result, map[string]interface{}{"type": "min-length-validator",
				"threshold": field.Min.(json.Number)})
		} else if field.Type == "integer" || field.Type == "float" {
			result = append(result, map[string]interface{}{"type": "min-number-value",
				"value": field.Min.(json.Number)})
		}
	}

	switch field.Max.(type) {
	case json.Number:
		if field.Type == "text" || field.Type == "password" || field.Type == "textarea" {
			result = append(result, map[string]interface{}{"type": "max-length-validator",
				"threshold": field.Max.(json.Number)})
		} else if field.Type == "integer" || field.Type == "float" {
			result = append(result, map[string]interface{}{"type": "max-number-value",
				"value": field.Max.(json.Number)})
		}
	}

	return result
}

func getOptions(field *Field) []map[string]interface{} {
	var result []map[string]interface{}
	var values []string
	switch field.Choices.(type) {
	case string:
		value := field.Choices.(string)
		if value == "" {
			return nil
		}
		values = strings.Split(value, "\n")
	case []string:
		values = field.Choices.([]string)
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
