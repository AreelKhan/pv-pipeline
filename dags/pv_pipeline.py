import sys
sys.path.append('/opt/airflow/dags/modules/') # hacky solution to import my ETL code

import logging
from pv_pipeline import PVExtract, PVTransform, PVLoad
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from os import path


default_args = {
    'owner': 'Areel',
    'start_date': datetime(2023, 12, 12),
    'retries': 0
}

dag = DAG(
    'pv-pipeline',
    default_args=default_args,
    description='A DAG to extract PV data from Parquet files in S3, transform with PySpark, and load into BigQuery',
    schedule_interval=None,  # does not run on schedule
    is_paused_upon_creation=False
)


def extract_pv(**kwargs):
    dag_run_conf = kwargs["dag_run"].conf
    extractor = PVExtract(
        aws_access_key_id=str(dag_run_conf.get("aws_access_key_id")),
        aws_secret_access_key=str(dag_run_conf.get("aws_secret_access_key")),
        region_name=str(dag_run_conf.get("region_name")),
        staging_area=str(dag_run_conf.get("staging_area")),
        logger=logging.getLogger(__name__)
    )
    extractor.extract(
        ss_id=int(dag_run_conf.get("ss_id")),
        start_date=datetime.strptime(dag_run_conf.get("start_date"), "%Y/%m/%d"),
        end_date=datetime.strptime(dag_run_conf.get("end_date"), "%Y/%m/%d")
    )
    return None

def load_pv(**kwargs):
    dag_run_conf = kwargs["dag_run"].conf
    staging_area = str(dag_run_conf.get("staging_area"))
    loader = PVLoad(
        project_id=str(dag_run_conf.get("bq_project_id")),
        credentials_path=path.join("dags", "modules", str(dag_run_conf.get("credentials_path"))),
        staging_area=staging_area,
        logger=logging.getLogger(__name__)
        )
    loader.load()
    return None


extract_task = PythonOperator(
    task_id='extract_pv',
    python_callable=extract_pv,
    provide_context=True,
    dag=dag,
)

spark_transform_task = SparkSubmitOperator(
    task_id='spark_transform_pv',
    conn_id="spark-conn",
    application="jobs/spark_transform_pv.py",
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_pv',
    python_callable=load_pv,
    provide_context=True,
    dag=dag,
)


extract_task >> spark_transform_task >> load_task
