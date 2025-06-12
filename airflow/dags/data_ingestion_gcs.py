import os
import logging
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET","trips_data_all")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def checkIfParquetExistsGCS(**kwargs) -> bool:
  hook = GCSHook()
  return not hook.exists(BUCKET, f"raw/{kwargs['parquet_name']}")

def checkIfBigQueryTableExists(**kwargs) -> bool:
  hook = BigQueryHook()
  return not hook.table_exists(project_id=PROJECT_ID, dataset_id=kwargs['dataset_name'], table_id=kwargs['table_name'])
  
with DAG(  
  dag_id = f"ingest_data_gcs",
  schedule= "0 6 2 * *",
  start_date=datetime(2021, 1, 1)
  ):
  
  # checkFinalBQTableExists = ShortCircuitOperator(
  #   task_id = "check_file_exists_local",
  #   python_callable=checkIfBigQueryTableExists,
  #   op_kwargs={'table_name':bq_final_table_name},
  # )    
    for taxi_type in ["yellow", "green", "fhv"]:  
      url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{{{{ logical_date.strftime(\'%Y-%m\') }}}}.parquet'
      parquet_name = f'{taxi_type}_{{{{ logical_date.strftime(\'%Y-%m\') }}}}.parquet'
      parquet_file = f'{AIRFLOW_HOME}/{parquet_name}'
      bq_external_table_name = f"{taxi_type}_external_{{{{ logical_date.strftime(\'%Y_%m\') }}}}"
      bq_staging_table_name = f"{taxi_type}_staging_{{{{ logical_date.strftime(\'%Y_%m\') }}}}"
      bq_final_table_name = f"{taxi_type}_final_{{{{ logical_date.strftime(\'%Y\') }}}}"
      DATASET_NAME = os.environ.get("DATASET_NAME") + taxi_type

      checkFileExistGCS = ShortCircuitOperator(
        task_id = f"check_file_exists_gcs_{taxi_type}",
        python_callable=checkIfParquetExistsGCS,
        op_kwargs={'parquet_name':parquet_name},
        trigger_rule = "none_failed",
        ignore_downstream_trigger_rules=False
      )
      
      curlTask = BashOperator(
        task_id = f"curl_parquet_{taxi_type}",
        bash_command=f'curl -sSL {url} > {parquet_file}'
      )

      uploadToGCS = LocalFilesystemToGCSOperator(
        task_id = f"upload_parquet_{taxi_type}",
        src = parquet_file,
        dst = f"raw/{parquet_name}",
        bucket=BUCKET
      )
      
      rmTask = BashOperator(
        task_id = f"rm_parquet_{taxi_type}",
        bash_command=f'rm {parquet_file}'
      )

      checkIfExternalTableExists = ShortCircuitOperator(
        task_id = f"check_if_external_table_exists_{taxi_type}",
        python_callable=checkIfBigQueryTableExists,
        op_kwargs={'dataset_name':'ny_taxi_extern', 'table_name':bq_external_table_name},
        trigger_rule = "none_failed",
        ignore_downstream_trigger_rules=False
      )
      
      createExternalTableBigQuery = BigQueryCreateTableOperator(
        project_id = PROJECT_ID,
        task_id = f"create_extern_table_{taxi_type}",
        dataset_id = 'ny_taxi_extern',
        table_id=bq_external_table_name,
        table_resource={
          "tableReference":{
            "projectId": PROJECT_ID,
            "datasetId": 'ny_taxi_extern',
            "tableId": bq_external_table_name
          },
          "type":"EXTERNAL",
          "externalDataConfiguration": {
            "sourceUris" : [f"gs://{BUCKET}/raw/{parquet_name}"],
            "sourceFormat" : "PARQUET"
          }
        },
      )
      
      # checkIfStagingTableExists = ShortCircuitOperator(
      #   task_id = f"check_if_staging_table_exists_{taxi_type}",
      #   python_callable=checkIfBigQueryTableExists,
      #   op_kwargs={'table_name':bq_staging_table_name},
      #   trigger_rule = "none_failed",
      #   ignore_downstream_trigger_rules=False,
      #   trigger_rule = "none_failed"
      # )

      # createStagingTable = BigQueryInsertJobOperator(
      #   task_id=f'create_staging_table_{taxi_type}',
      #   configuration={
      #       'query': {
      #           'query': f"""
      #               CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.{bq_staging_table_name}` AS
      #               SELECT 
      #                   MD5(CONCAT(
      #                       COALESCE(CAST(VendorID AS STRING), ""),
      #                       COALESCE(CAST(tpep_pickup_datetime AS STRING), ""),
      #                       COALESCE(CAST(tpep_dropoff_datetime AS STRING), ""),
      #                       COALESCE(CAST(PULocationID AS STRING), ""),
      #                       COALESCE(CAST(DOLocationID AS STRING), "")
      #                   )) AS unique_row_id,
      #                   "{parquet_name}" AS filename,
      #                   *
      #                   FROM `{PROJECT_ID}.{DATASET_NAME}.{bq_external_table_name}`
      #           """,
      #           'useLegacySql': False,
      #       }
      #   }
      # )
      
      # createFinalTable = BigQueryInsertJobOperator(
      #   task_id=f"create_final_table_{taxi_type}",
      #   configuration={
      #       "query": {
      #           "query": f"""
      #               CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_NAME}.{bq_final_table_name}` 
      #               LIKE `{PROJECT_ID}.{DATASET_NAME}.{bq_staging_table_name}`
      #           """,
      #           "useLegacySql": False,
      #       }
      #   },
      #   trigger_rule = "none_failed"
      # )
      
      # MergeIntoFinalTable = BigQueryInsertJobOperator(
      #   task_id=f"merge_final_table_{taxi_type}",
      #   configuration={
      #       "query": {
      #           "query": f"""
      #               MERGE INTO `{PROJECT_ID}.{DATASET_NAME}.{bq_final_table_name}` AS F
      #               USING `{PROJECT_ID}.{DATASET_NAME}.{bq_staging_table_name}` AS S
      #               ON F.unique_row_id = S.unique_row_id
      #               WHEN NOT MATCHED THEN 
      #                 INSERT ROW
      #           """,
      #           "useLegacySql": False,
      #       }
      #   },
      # )
      
      # checkFinalBQTableExists >> checkFileExistGCS >> curlTask >> uploadToGCS >> rmTask >> \
      # checkIfExternalTableExists >> createExternalTableBigQuery >> checkIfStagingTableExists >> createStagingTable >> \
      # createFinalTable >> MergeIntoFinalTable
    
      checkFileExistGCS >> curlTask >> uploadToGCS >> rmTask >> checkIfExternalTableExists >> createExternalTableBigQuery