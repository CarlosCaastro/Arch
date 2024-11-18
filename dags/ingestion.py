import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark-master:7077"

spark_app_name = "INGESTION"
now = datetime.now()

default_args = {
    "owner": "Carlos",
    "start_date": datetime(now.year, now.month, now.day),
}

dag = DAG(
    dag_id="ingestion",
    description="This DAG triggers a Spark job to process ingestion data from NYC gov and save in MinIO",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags = ["INGESTION"]
)

start = DummyOperator(task_id="start", dag=dag)

ingest = SparkSubmitOperator(
    task_id="ingest",
    application="jobs/ingest.py",
    name=spark_app_name,
    conn_id=spark_conn,
    verbose=1,
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

start >> ingest >> end