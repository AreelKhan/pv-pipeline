import sys
sys.path.append('/opt/airflow/dags/modules/') # hacky solution to import my ETL code

import logging
from pv_metadata_pipeline import MetadataExtract, MetadataTransform, MetadataLoad
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from os import path


default_args = {
    'owner': 'Areel',
    'start_date': datetime(2023, 12, 12),
    'retries': 0
}

dag = DAG(
    'pv-metadata-pipeline',
    default_args=default_args,
    description='A simple DAG to extract PV metadata data from Parquet files in S3, transform, and load into BigQuery',
    schedule_interval=None,  # does not run on schedule
    is_paused_upon_creation=False
)


def extract_metadata(**kwargs):
    dag_run_conf = kwargs["dag_run"].conf
    extractor = MetadataExtract(
        aws_access_key_id=str(dag_run_conf.get("aws_access_key_id")),
        aws_secret_access_key=str(dag_run_conf.get("aws_secret_access_key")),
        region_name=str(dag_run_conf.get("region_name")),
        staging_area=str(dag_run_conf.get("staging_area")),
        logger=logging.getLogger(__name__)
    )
    extractor.extract()
    return None

def transform_metadata(**kwargs):
    dag_run_conf = kwargs["dag_run"].conf
    transformer = MetadataTransform(
        staging_area=str(dag_run_conf.get("staging_area")),
        logger=logging.getLogger(__name__)
    )
    transformer.transform()
    return None

def load_metadata(**kwargs):
    dag_run_conf = kwargs["dag_run"].conf
    staging_area = str(dag_run_conf.get("staging_area"))
    loader = MetadataLoad(
        project_id=str(dag_run_conf.get("bq_project_id")),
        credentials_path=path.join("dags", "modules", str(dag_run_conf.get("credentials_path"))),
        staging_area=staging_area,
        logger=logging.getLogger(__name__)
        )
    loader.load()
    return None

extract_task = PythonOperator(
    task_id='extract_metadata',
    python_callable=extract_metadata,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_metadata',
    python_callable=transform_metadata,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_metadata',
    python_callable=load_metadata,
    provide_context=True,
    dag=dag,
)


extract_task >> transform_task >> load_task
