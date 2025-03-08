from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="first_dag",
    schedule="0 8 * * *",
    start_date=datetime(2025, 3, 7),
    catchup=False,
):

    task1 = BashOperator(
        task_id="task1",
        bash_command='echo "Hello, Airflow!"'
    )
    task2 = BashOperator(
        task_id="task2",
        bash_command='date'
    )
    task3 = BashOperator(
        task_id="task3",
        bash_command='echo "Goodbye, Airflow!"'
    )

    (
        task1 >> task2 >> task3
    )
