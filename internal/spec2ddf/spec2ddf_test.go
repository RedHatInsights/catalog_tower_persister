package spec2ddf

import (
	"context"
	"strings"
	"testing"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/testhelper"
	"github.com/stretchr/testify/assert"
)

var text1 = `{
    "name": "",
    "description": "",
    "spec": [
        {
            "question_name": "Hobbies",
            "question_description": "Select your hobbies",
            "required": true,
            "type": "multiselect",
            "variable": "hobbies",
            "min": null,
            "max": null,
            "default": "Cricket",
            "choices": "Lawn Tennis\nCycling\nTable Tennis\nCricket\nFlying Kites",
            "new_question": true
        }
	]
}`

var text2 = `{
    "name": "",
    "description": "",
    "spec": [
       {
            "question_name": "Age",
            "question_description": "Enter your age",
            "required": true,
            "type": "integer",
            "variable": "age",
            "min": 0,
            "max": 100,
            "default": 34,
            "choices": "",
            "new_question": true
        }
	]
}`

var text3 = `{
    "name": "",
    "description": "",
    "spec": [
       {
            "question_name": "Enter Temperature",
            "question_description": "Please Enter Temperature",
            "required": true,
            "type": "float",
            "variable": "temperature",
            "min": 0,
            "max": 100,
            "default": 98.6,
            "choices": "",
            "new_question": true
        }
	]
}`

var text4 = `{
    "name": "",
    "description": "",
    "spec": [
       {
            "question_name": "Script",
            "question_description": "Your Script",
            "required": true,
            "type": "textarea",
            "variable": "script",
            "min": 0,
            "max": 4096,
            "default": "puts \"Hello World\"",
            "choices": "",
            "new_question": true
        }
     ]
}`

var text5 = `{
    "name": "",
    "description": "",
    "spec": [
      {
            "question_name": "Password",
            "question_description": "Please enter your password",
            "required": true,
            "type": "password",
            "variable": "blank_password",
            "min": 0,
            "max": 32,
            "default": "$encrypted$",
            "choices": "",
            "new_question": true
        }
       ]
}`

var text6 = `{
    "name": "",
    "description": "",
    "spec": [
        {
            "question_name": "Username",
            "question_description": "Please enter Username",
            "required": true,
            "type": "text",
            "variable": "username",
            "min": 0,
            "max": 1024,
            "default": "Fred_Flintstone",
            "choices": "",
            "new_question": true
        }
       ]
}`

var text7 = `{
    "name": "",
    "description": "",
    "spec": [
       {
            "question_name": "Cost Factor",
            "question_description": "Please Select a cost factor",
            "required": true,
            "type": "multiplechoice",
            "variable": "cost_factor",
            "min": null,
            "max": null,
            "default": "34.6",
            "choices": "34.6",
            "new_question": true
        }
       ]
}`

var text8 = `{
    "name": "",
    "description": "",
    "spec": [
       {
            "question_name": "Cost Factor",
            "question_description": "Please Select a cost factor",
            "required": true,
            "type": "gobbledegook",
            "variable": "cost_factor",
            "min": null,
            "max": null,
            "default": "34.6",
            "choices": "34.6",
            "new_question": true
        }
       ]
}`

var text9 = `{
    "name": "",
    "description": "",
    "spec": [
       {
            "question_name": "CPU",
            "question_description": "Select a CPU",
            "required": true,
            "type": "multiplechoice",
            "variable": "cpu",
            "min": null,
            "max": null,
            "default": "",
            "choices": "3"
        }
       ]
}`

var specTests = []struct {
	data     string
	name     string
	ddfMatch string
	errMsg   string
}{
	{text1, "TestMultiSelect", `"component":"select-field"`, ""},
	{text2, "TestInteger", `"dataType":"integer"`, ""},
	{text3, "TestFloat", `"dataType":"float"`, ""},
	{text4, "TestTextArea", `"component":"textarea-field"`, ""},
	{text5, "TestPassword", `"component":"text-field"`, ""},
	{text6, "TestText", `"component":"text-field"`, ""},
	{text7, "TestMultipleChoice", `"component":"select-field"`, ""},
	{text8, "TestBadFieldType", "", "Unsupported field type"},
	{text9, "TestMultipleChoiceNoDefault", `"component":"select-field"`, ""},
}

func TestSpec(t *testing.T) {
	for _, tt := range specTests {
		ctx := context.TODO()
		c := &Converter{}
		b, err := c.Convert(ctx, testhelper.TestLogger(), strings.NewReader(tt.data))
		if len(tt.errMsg) == 0 {
			assert.Equal(t, strings.Contains(string(b), tt.ddfMatch), true, tt.ddfMatch)
			assert.Nil(t, err, tt.name)
		} else {
			assert.NotNil(t, err, tt.name)
			if !strings.Contains(err.Error(), tt.errMsg) {
				t.Fatalf("Error message should have contained %s", tt.errMsg)
			}
		}
	}
}
