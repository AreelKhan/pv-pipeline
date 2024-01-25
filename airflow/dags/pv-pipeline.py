import sys
sys.path.append('/opt/airflow/dags/modules/') # hacky solution to import my ETL code

import logging
import os
from extract import PVExtract
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Areel',
    'depends_on_past': False, # TODO remove
    'start_date': datetime(2023, 12, 12),
    'email_on_failure': False, # TODO remove
    'email_on_retry': False, # TODO remove
    'retries': 1, # TODO remove
    'retry_delay': timedelta(minutes=5), # TODO remove
}

dag = DAG(
    'pv-pipeline',
    default_args=default_args,
    description='A simple DAG to extract data from Parquet files in S3, transform, and load into BigQuery',
    schedule_interval=None,  # does not run on schedule
    is_paused_upon_creation=False
)

def extract_pv(**kwargs):
    log = logging.getLogger(__name__)

    dag_run_conf = kwargs["dag_run"].conf
    ss_id: int = int(dag_run_conf.get("ss_id"))
    start_date: datetime = datetime.strptime(dag_run_conf.get("start_date"), "%Y/%m/%d")
    end_date: datetime = datetime.strptime(dag_run_conf.get("end_date"), "%Y/%m/%d")
    staging_area: str = str(dag_run_conf.get("staging_area"))
    aws_access_key_id: str = str(dag_run_conf.get("aws_access_key_id"))
    aws_secret_access_key: str = str(dag_run_conf.get("aws_secret_access_key"))
    region_name: str = str(dag_run_conf.get("region_name"))
    log.info(os.getcwd())
    extractor = PVExtract(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        staging_area=staging_area,
        logger=log
    )
    extractor.extract(ss_id, start_date, end_date)
    

extract_task = PythonOperator(
    task_id='extract_pv',
    python_callable=extract_pv,
    provide_context=True,
    dag=dag,
)

"""
upload_to_gcs_task = S3ToGoogleCloudStorageTransferOperator(
    task_id='upload_to_gcs',
    bucket_name='your-gcs-bucket',
    object_name='path/to/transformed_file.parquet',
    filename='/path/to/transformed_file.parquet',
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag,
)

load_to_bq_task = GoogleCloudStorageToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket='your-gcs-bucket',
    source_objects=['path/to/transformed_file.parquet'],
    destination_project_dataset_table='your_project.your_dataset.your_table',
    schema_fields=[{'name': 'new_column', 'type': 'INTEGER'},  # Add the schema fields accordingly
                   # Add other fields as needed
                   ],
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    bigquery_conn_id='bigquery_default',
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag,
)
"""

extract_task
