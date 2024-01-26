import sys
sys.path.append('/opt/airflow/dags/modules/') # hacky solution to import my ETL code

import logging
from pv_metadata_pipeline import MetadataExtract
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Areel',
    'start_date': datetime(2023, 12, 12),
    'retries': 1, # TODO remove
    'retry_delay': timedelta(minutes=5), # TODO remove
}

dag = DAG(
    'pv-metadata-pipeline',
    default_args=default_args,
    description='A simple DAG to extract PV metadata data from Parquet files in S3, transform, and load into BigQuery',
    schedule_interval=None,  # does not run on schedule
    is_paused_upon_creation=False
)


def extract_metadata(**kwargs):
    log = logging.getLogger(__name__)
    
    dag_run_conf = kwargs["dag_run"].conf
    staging_area: str = str(dag_run_conf.get("staging_area"))
    aws_access_key_id: str = str(dag_run_conf.get("aws_access_key_id"))
    aws_secret_access_key: str = str(dag_run_conf.get("aws_secret_access_key"))
    region_name: str = str(dag_run_conf.get("region_name"))

    extractor = MetadataExtract(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        staging_area=staging_area,
        logger=log
    )
    extractor.extract()
    return None

def transform_metadata(**kwargs):
    return None


def load_metadata(**kwargs):
    return None


extract_task = PythonOperator(
    task_id='extract_pv',
    python_callable=extract_metadata,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_pv',
    python_callable=transform_metadata,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_pv',
    python_callable=load_metadata,
    provide_context=True,
    dag=dag,
)


extract_task >> transform_task >> load_task
