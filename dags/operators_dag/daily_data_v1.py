from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from operators_dag.raw.file_class import FileProcessor
#from airflow.operators.email import EmailOperator

original_source = '/opt/airflow/dags/operators_dag/data/original/'
updated_source = '/opt/airflow/dags/operators_dag/data/updated/'

file_process = FileProcessor(original_source, updated_source)

def create_file(ds):
    ds_date = datetime.strptime(ds, "%Y-%m-%d")
    file_name = f'sales_data_{ds_date.strftime("%d-%m-%Y")}.txt'
    content = 'First line of data!'
    file_process.create_file(file_name, content)

def update_file(ds):
    ds_date = datetime.strptime(ds, "%Y-%m-%d")
    file_name = f'sales_data_{ds_date.strftime("%d-%m-%Y")}.txt'
    content = '\nSecond line of data!'
    file_process.update_file(file_name, content)

def delete_file(ds):
    ds_date = datetime.strptime(ds, "%Y-%m-%d")
    file_name = f'sales_data_{ds_date.strftime("%d-%m-%Y")}.txt'
    file_process.delete_file(file_name)

with DAG(
    dag_id="daily_sales_dag_v1",
    schedule=None, #"0 8 * * *",
    start_date=datetime(2025, 3, 9),
    catchup=False,
):

    create_file_task = PythonOperator(
        task_id="create_file_task",
        python_callable=create_file,
        provide_context=True,
        op_kwargs={"current_dag_run_date": "{{ds}}"},
    )
    
    update_file_task = PythonOperator(
        task_id="update_file_task",
        python_callable=update_file,
        provide_context=True,
        op_kwargs={"current_dag_run_date": "{{ds}}"},
    )

    delete_file_task = PythonOperator(
        task_id="delete_file_task",
        python_callable=delete_file,
        provide_context=True,
        op_kwargs={"current_dag_run_date": "{{ds}}"},
    )

    create_file_task >> update_file_task >> delete_file_task
    
