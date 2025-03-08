from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
#from airflow.utils.task_group import TaskGroup

files = ["file1.txt", "file2.txt", "file3.txt"]

for file in files:
    with DAG(
        dag_id=f"dag_process_{file}",
        schedule_interval=None, #schedule="0 8 * * *",
        start_date=datetime(2025, 3, 8),
        catchup=False,
    ):

        task1 = BashOperator(
            task_id=f"read_{file}",
            bash_command=f"echo 'Reading file: {file}' && cat /opt/airflow/dags/generation_dag/files/{file} || echo 'File not found'",
        )
        
        task2 = BashOperator(
            task_id=f"processing_{file}",
            bash_command=f"echo 'processing {file}'",
        )

        task3 = BashOperator(
            task_id=f"save_{file}",
            bash_command=f"echo 'save {file}'",
        )

        task1 >> task2 >> task3

