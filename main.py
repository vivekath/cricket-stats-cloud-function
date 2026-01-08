from googleapiclient.discovery import build
import traceback
from google.cloud import bigquery

# def load_data_bigquery(event, context=None):
#     try:
#         # Handle both CloudEvent and dict
#         if hasattr(event, "data"):
#             event_data = event.data          # CloudEvent
#         else:
#             event_data = event               # dict

#         print(event_data)

#         bucket = event_data["bucket"]
#         name = event_data["name"]

#         service = build("dataflow", "v1b3")
#         project = "quantum-episode-345713"

#         template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

#         template_body = {
#             "jobName": "bq-load",
#             "parameters": {
#                 "javascriptTextTransformGcsPath": f"gs://{bucket}/udf.js",
#                 "JSONPath": f"gs://{bucket}/bq.json",
#                 "javascriptTextTransformFunctionName": "transform",
#                 "outputTable": f"{project}:cricket_dataset.icc_odi_batsman_ranking",
#                 "inputFilePattern": f"gs://{bucket}/{name}",
#                 "bigQueryLoadingTemporaryDirectory": f"gs://{bucket}"
#             }
#         }

#         request = service.projects().templates().launch(
#             projectId=project,
#             gcsPath=template_path,
#             body=template_body
#         )

#         response = request.execute()
#         print(response)

#     except Exception as e:
#         print("❌ Error occurred in load_data_bigquery")
#         print(str(e))
#         traceback.print_exc()
#         raise


def load_data_bigquery(event, context=None):
    try:
        # Handle both CloudEvent and dict
        if hasattr(event, "data"):
            event_data = event.data          # CloudEvent
        else:
            event_data = event               # dict

        print(event_data)

        bucket = event_data["bucket"]
        name = event_data["name"]
        gcs_uri = f"gs://{bucket}/{name}"

        print(f"Triggered by file: {gcs_uri}")

        # BigQuery client
        client = bigquery.Client()
        dataset_id = "cricket_dataset"
        table_id = "icc_odi_batsman_ranking"
        project = client.project  # Or specify your project

        table_ref = f"{project}.{dataset_id}.{table_id}"

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,  # or JSON
            skip_leading_rows=1,  # if CSV has header
            autodetect=True       # auto-detect schema
        )

        # Start load job
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )

        print(f"Starting job {load_job.job_id}")
        load_job.result()  # Waits for job to complete
        print(f"Loaded {load_job.output_rows} rows into {table_ref}")

    except Exception as e:
        print("❌ Error occurred in load_data_bigquery")
        print(str(e))
        traceback.print_exc()
        raise