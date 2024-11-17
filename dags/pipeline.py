from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta

now = datetime.now()
default_args = {
    'owner': 'Carlos',
    "start_date": datetime(now.year, now.month, now.day),
}

dags_a_serem_executadas = ['ingestion', 'landing_to_bronze', 'bronze_to_silver', 'd_calendar', 'f_yellow_taxi'] 

with DAG(
    'master_dag',
    default_args=default_args,
    description='DAG para disparar outras DAGs com base em uma lista',
    schedule_interval="0 7 * * *",
    catchup=False,
    is_paused_upon_creation=False,
    tags = ["PIPE"]
) as dag:

    trigger_tasks = []

    for dag_id in dags_a_serem_executadas:
        trigger_task = TriggerDagRunOperator(
            task_id=f'trigger_{dag_id}',
            trigger_dag_id=dag_id,
            wait_for_completion=True,
            poke_interval=20
        )
        trigger_tasks.append(trigger_task)

    for i in range(len(trigger_tasks) - 1):
        trigger_tasks[i] >> trigger_tasks[i + 1]
