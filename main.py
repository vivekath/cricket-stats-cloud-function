from googleapiclient.discovery import build

def load_data_bigquery(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "quantum-episode-345713"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bq-load",  # Provide a unique name for the job
        "parameters": {
        "javascriptTextTransformGcsPath": "gs://cricbuzz-stats-08012026/udf.js",
        "JSONPath": "gs://cricbuzz-stats-08012026/bq.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "prj-poc-001:cricket_dataset.icc_odi_batsman_ranking",
        "inputFilePattern": "gs://cricbuzz-stats-08012026/batsmen_rankings.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://cricbuzz-stats-08012026",
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)

