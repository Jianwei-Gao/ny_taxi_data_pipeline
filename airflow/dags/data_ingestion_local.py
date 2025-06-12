from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime
import os 
from ingest_script import ingest_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
PG_HOST = os.environ.get('PG_HOST')
PG_USER = os.environ.get('PG_USER')
PG_PASSWORD = os.environ.get('PG_PASSWORD')
PG_PORT = os.environ.get('PG_PORT')
PG_DATABASE = os.environ.get('PG_DATABASE')

url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ logical_date.strftime(\'%Y-%m\') }}.parquet'
output = f'{AIRFLOW_HOME}/output_{{{{ logical_date.strftime(\'%Y-%m\') }}}}.parquet'
table_name = 'yellow_taxi_{{ logical_date.strftime(\'%Y-%m\') }}'

local_workflow = DAG(
  dag_id = "ingest_data_local",
  schedule= "0 6 2 * *",
  start_date=datetime(2021, 1, 1)
)

with local_workflow:
  curl_task = BashOperator(
    task_id = 'wget',
    bash_command=f'curl -sSL {url} > {output}'
  )
  
  ingest_task = PythonOperator(
    task_id = 'ingest',
    python_callable=ingest_callable,
    op_kwargs=dict(
      user=PG_USER,
      password=PG_PASSWORD,
      host=PG_HOST,
      port=PG_PORT,
      db=PG_DATABASE,
      table_name=table_name,
      file=output
    )
  )
  
  cleanup = BashOperator(
    task_id = 'rm',
    bash_command=f'rm {output}'
  )
  curl_task >> ingest_task
