import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET","trips_data_all")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
DATASET_NAME = os.environ.get("DATASET_NAME")

url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ logical_date.strftime(\'%Y-%m\') }}.parquet'
parquet_name = 'yellow_{{ logical_date.strftime(\'%Y-%m\') }}.parquet'
parquet_file = f'{AIRFLOW_HOME}/{parquet_name}'
bq_external_table_name = "yellow_external_{{ logical_date.strftime(\'%Y_%m\') }}"
bq_staging_table_name = "yellow_staging_{{ logical_date.strftime(\'%Y_%m\') }}"
bq_final_table_name = "yellow_final_{{ logical_date.strftime(\'%Y\') }}"

def checkIfParquetExistsLocal(**kwargs) -> bool:
  return not os.path.isfile(kwargs['parquet_file'])

def checkIfParquetExistsGCS(**kwargs) -> bool:
  hook = GCSHook()
  return not hook.exists(BUCKET, f"raw/{kwargs['parquet_name']}")

with DAG(  
  dag_id = "ingest_data_gcs",
  schedule= "0 6 2 * *",
  start_date=datetime(2021, 1, 1)
  ):
  
  checkFileExistLocal = ShortCircuitOperator(
    task_id = "check_file_exists_local",
    python_callable=checkIfParquetExistsLocal,
    op_kwargs={'parquet_file':parquet_file},
    ignore_downstream_trigger_rules=False
  )    
  
  curlTask = BashOperator(
    task_id = "curl_parquet",
    bash_command=f'curl -sSL {url} > {parquet_file}'
  )

  checkFileExistGCS = ShortCircuitOperator(
    task_id = "check_file_exists_gcs",
    python_callable=checkIfParquetExistsGCS,
    op_kwargs={'parquet_name':parquet_name},
    trigger_rule = "none_failed",
    ignore_downstream_trigger_rules=False
  )
  
  uploadToGCS = LocalFilesystemToGCSOperator(
    task_id = "upload_parquet",
    src = parquet_file,
    dst = f"raw/{parquet_name}",
    bucket=BUCKET
  )
  
  createExternalTableBigQuery = BigQueryCreateTableOperator(
    project_id = PROJECT_ID,
    task_id = "create_extern_table",
    dataset_id = DATASET_NAME,
    table_id=bq_external_table_name,
    table_resource={
      "tableReference":{
        "projectId": PROJECT_ID,
        "datasetId": DATASET_NAME,
        "tableId": bq_external_table_name
      },
      "type":"EXTERNAL",
      "externalDataConfiguration": {
        "sourceUris" : [f"gs://{BUCKET}/raw/{parquet_name}"],
        "sourceFormat" : "PARQUET"
      }
    },
    if_exists = "skip",
    trigger_rule = "none_failed",
  )
  
  createStagingTable = BigQueryInsertJobOperator(
    task_id='create_staging_table',
    configuration={
        'query': {
            'query': f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.{bq_staging_table_name}` AS
                SELECT 
                    MD5(CONCAT(
                        COALESCE(CAST(VendorID AS STRING), ""),
                        COALESCE(CAST(tpep_pickup_datetime AS STRING), ""),
                        COALESCE(CAST(tpep_dropoff_datetime AS STRING), ""),
                        COALESCE(CAST(PULocationID AS STRING), ""),
                        COALESCE(CAST(DOLocationID AS STRING), "")
                    )) AS unique_row_id,
                    "{parquet_name}" AS filename,
                    *
                    FROM `{PROJECT_ID}.{DATASET_NAME}.{bq_external_table_name}`
            """,
            'useLegacySql': False,
        }
    },
    trigger_rule = "none_failed"
  )
  
  createFinalTable = BigQueryInsertJobOperator(
    task_id="create_final_table",
    configuration={
        "query": {
            "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_NAME}.{bq_final_table_name}` 
                LIKE `{PROJECT_ID}.{DATASET_NAME}.{bq_staging_table_name}`
            """,
            "useLegacySql": False,
        }
    },
  )
  
  MergeIntoFinalTable = BigQueryInsertJobOperator(
    task_id="merge_final_table",
    configuration={
        "query": {
            "query": f"""
                MERGE INTO `{PROJECT_ID}.{DATASET_NAME}.{bq_final_table_name}` AS F
                USING `{PROJECT_ID}.{DATASET_NAME}.{bq_staging_table_name}` AS S
                ON F.unique_row_id = S.unique_row_id
                WHEN NOT MATCHED THEN 
                  INSERT ROW
            """,
            "useLegacySql": False,
        }
    },
  )
  
  checkFileExistLocal >> curlTask >> checkFileExistGCS >> uploadToGCS >> createExternalTableBigQuery >> \
  createStagingTable >> createFinalTable >> MergeIntoFinalTable
  
