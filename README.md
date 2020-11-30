# Catalog Tower Persister

Catalog Tower Persister adds/updates/removes Ansible Tower objects in the Catalog Inventory
database. It gets information about these objects in a compressed tar file from the Upload Service.
Once the Catalog MQTT Client uploads the file, the Upload Service sends a message on the kafka
topic **platform.upload.catalog** with the payload details and a URL to download the file.

Kafka Payload info
```json
{
    "size": 12345,
    "url": "https://....",
    "service": "catalog",
    "request_id": "xxx-yyy-nnn",
    "metadata": {
        "reporter": "",
        "stale_timestamp": ""
    },
    "b64_identity": "xxxxxxxx",
    "timestamp": "2020-11-25T16:22:56.449719273Z",
    "category": "catalog",
    "account": "xxxx",
    "principal": "xxxx"
}
```

Layout of the tar file for full refresh
```
api/v2/job_templates/page1.json
api/v2/job_templates/page2.json
api/v2/job_templates/page3.json
api/v2/job_templates/10/survey_spec/page1.json
api/v2/job_templates/17/survey_spec/page1.json
api/v2/credential_types/page1.json
api/v2/credentials/page1.json
api/v2/inventories/page1.json
api/v2/workflow_job_templates/page1.json
api/v2/workflow_job_templates/page2.json
api/v2/workflow_job_templates/12/survey_spec/page1.json
api/v2/workflow_job_templates/20/survey_spec/page1.json
api/v2/workflow_job_template_nodes/page1.json
api/v2/workflow_job_template_nodes/page2.json
```

Possible layout of the tar file for incremental refresh
The id file carries the ids of all the objects so we can 
delete the ones that no longer exist in the tower
```
api/v2/job_templates/page1.json
api/v2/job_templates/id1.json
api/v2/job_templates/30/survey_spec/page1.json
api/v2/workflow_job_templates/page1.json
api/v2/workflow_job_templates/page2.json
api/v2/workflow_job_templates/12/survey_spec/page1.json
api/v2/workflow_job_templates/20/survey_spec/page1.json
api/v2/workflow_job_template_nodes/page1.json
api/v2/workflow_job_template_nodes/page2.json
```

![Alt Upload](image.png?raw=true)
