from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="test_spark_submit",
    default_args={
        "owner": "Areel Khan"
    }
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs Started"),
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Job completed"),
    dag=dag
)

start >> python_job >> end