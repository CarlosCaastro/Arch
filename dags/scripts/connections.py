from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow import settings

def create_connections():
    session = settings.Session()

    postgres_conn = Connection(
        conn_id='postgres_conn',
        conn_type='postgres',
        host='postgres',
        schema='bix_challenger',
        login='airflow',
        password='airflow',
        port=5432
    )

    spark_conn = Connection(
        conn_id='spark_conn',
        conn_type='spark',
        host='spark://spark-master',
        port=7077,
        extra='{"deploy_mode": "client", "spark_binary": "spark-submit"}'
    )

    existing_postgres_conn = session.query(Connection).filter(Connection.conn_id == postgres_conn.conn_id).first()
    existing_spark_conn = session.query(Connection).filter(Connection.conn_id == spark_conn.conn_id).first()

    if not existing_postgres_conn:
        session.add(postgres_conn)
    if not existing_spark_conn:
        session.add(spark_conn)

    session.commit()

if __name__ == "__main__":
    create_connections()
