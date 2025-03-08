from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="taskgroup_example",
    schedule="0 8 * * *",
    start_date=datetime(2025, 3, 8),
    catchup=False,
):

    with TaskGroup(group_id='group1') as group1:
        task1 = BashOperator(
            task_id="task1",
            bash_command='echo "Hello, Airflow!"'
        )
        task2 = BashOperator(
            task_id="task2",
            bash_command='date'
        )

        task1 >> task2
    
    with TaskGroup(group_id='group2') as group2:
        task3 = BashOperator(
            task_id="task3",
            bash_command='echo "Goodbye, Airflow!"'
        )

        task3  

    group1 >> group2

