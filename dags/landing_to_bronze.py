import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

spark_conn = os.environ.get("spark_conn", "spark_conn")
spark_master = "spark://spark-master:7077"

spark_app_name = "LANDING_TO_BRONZE"
now = datetime.now()

default_args = {
    "owner": "Carlos",
    "start_date": datetime(now.year, now.month, now.day),
}

dag = DAG(
    dag_id="landing_to_bronze",
    description="This DAG triggers a Spark job to process data from MinIO (Delta Lake) and write at layer Bronze in MinIO",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags = ["LANDING","BRONZE"]
)

start = DummyOperator(task_id="start", dag=dag)

landing_to_bronze = SparkSubmitOperator(
    task_id="landing_to_bronze",
    application="jobs/landing_to_bronze.py",
    name=spark_app_name,
    conn_id=spark_conn,
    verbose=1,
    conf = {
        "spark.master": spark_master,
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "minio",
        "spark.hadoop.fs.s3a.secret.key": "minio123",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.warehouse.dir": "s3a://ifood/warehouse",
        "spark.sql.parquet.enableVectorizedReader": "false",
        "spark.sql.catalogImplementation": "hive",
        "javax.jdo.option.ConnectionURL": "jdbc:postgresql://postgres:5432/airflow",
        "javax.jdo.option.ConnectionUserName": "airflow",
        "javax.jdo.option.ConnectionPassword": "airflow"
    },
    jars=",".join([f"/opt/bitnami/spark/jars/{jar}" for jar in os.listdir('/opt/bitnami/spark/jars')]),
    packages="io.delta:delta-spark_2.12:3.2.0,io.delta:delta-storage:3.2.0",
    driver_class_path="/opt/bitnami/spark/jars/postgresql-42.2.20.jar",
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

start >> landing_to_bronze >> end