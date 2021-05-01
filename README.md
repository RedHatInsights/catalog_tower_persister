# Catalog Tower Persister

Catalog Tower Persister adds/updates/removes Ansible Tower objects in the Catalog Inventory
database. It works as a helper to the Catalog Inventory Service. The Catalog Inventory service 
doles out work to the persister. Catalog Inventory Service is responsible for managing the inventory collection process. It sends out inventory collection request to the remote catalog worker. It tracks upload of the tar files from the remote catalog worker delivered via the Ingress Service. Once the payload has been deposited in the S3 bucket by the Ingress service, it sends a Kafka message to the Catalog Inventory service, which then sends a Kafka message to the Persister with the payload details.

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
    "category": "<<refresh_task_id>>",
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

![Alt UsingUploadService](./docs/ctp.png?raw=true)
