import sys
sys.path.append('/opt/airflow/dags/modules/') # hacky solution to import my ETL code

from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    "owner": "Areel",
    "start_date": datetime(2023, 12, 12),
    "retries": 0
}

dag = DAG(
    "test-spark-submit",
    default_args=default_args,
    description="testing spark submit",
    schedule_interval=None,  # does not run on schedule
    is_paused_upon_creation=False
)



load_task = SparkSubmitOperator(
    task_id='load_metadata',
    python_callable=load_metadata,
    provide_context=True,
    dag=dag,
)


extract_task >> transform_task >> load_task
