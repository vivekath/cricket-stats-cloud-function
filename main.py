from googleapiclient.discovery import build
import traceback

def load_data_bigquery(event, context=None):   
    try:
        # Handle both CloudEvent and dict
        if hasattr(event, "data"):
            event_data = event.data          # CloudEvent
        else:
            event_data = event               # dict

        print(event_data)

        bucket_name = event_data["bucket"]

         # Extract event info (optional)
        bucket = event_data.data["bucket"]
        name = event_data.data["name"]

        service = build("dataflow", "v1b3")
        project = "quantum-episode-345713"

        template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

        template_body = {
            "jobName": "bq-load",
            "parameters": {
                "javascriptTextTransformGcsPath": f"gs://{bucket_name}/udf.js",
                "JSONPath": f"gs://{bucket_name}/bq.json",
                "javascriptTextTransformFunctionName": "transform",
                "outputTable": f"{project}:cricket_dataset.icc_odi_batsman_ranking",
                "inputFilePattern": f"gs://{bucket}/{name}",
                "bigQueryLoadingTemporaryDirectory": f"gs://{bucket_name}"
            }
        }

        request = service.projects().templates().launch(
            projectId=project,
            gcsPath=template_path,
            body=template_body
        )

        response = request.execute()
        print(response)

    except Exception as e:
        print("❌ Error occurred in load_data_bigquery")
        print(str(e))
        traceback.print_exc()
        raise
