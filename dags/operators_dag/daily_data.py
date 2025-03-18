from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
#from generation_dag.raw.wb_postgres import WBLoader
import logging
import shutil
import os

logger = logging.getLogger(__name__)
#logger.setLevel(logging.INFO)

def update_file(ds):
    logger.info(f"Processing file for date: {ds}")
    ds_date = datetime.strptime(ds, "%Y-%m-%d")
    file_name = f'sales_data_{ds_date.strftime("%d-%m-%Y")}.txt'

    source_file = 'dags/operators_dag/data/original/' + file_name
    destination_folder = "dags/operators_dag/data/updated"
    copy_file = os.path.join(destination_folder, file_name)

    # Проверяем, что файл существует
    if not os.path.exists(source_file):
        logger.error(f"File {source_file} does not exist")
        raise FileNotFoundError(f"File {source_file} does not exist")

    # Создаем папку, если её нет
    os.makedirs(destination_folder, exist_ok=True)

    logger.info(f"Copying file from {source_file} to {copy_file}")
    shutil.copy(source_file, copy_file)

    logger.info("Appending text to the file")
    with open(copy_file, "a") as file:
        file.write("\nSome text")

with DAG(
    dag_id="daily_sales_dag",
    schedule=None, #"0 8 * * *",
    start_date=datetime(2025, 3, 9),
    catchup=False,
):

    create_file_task = BashOperator(
        task_id="create_file_task",
        bash_command='echo "Sales data for $(date +"%d-%m-%Y")" > /opt/airflow/dags/operators_dag/data/original/sales_data_$(date +"%d-%m-%Y").txt'
    )
    
    update_file_task = PythonOperator(
        task_id="update_file_task",
        python_callable=update_file,
        provide_context=True,
        # op_kwargs={"current_dag_run_date": "{{ds}}"},

    )

    clear_data_task = BashOperator(
        task_id="clear_data_task",
        bash_command='rm -f /opt/airflow/dags/operators_dag/data/original/sales_data_$(date +"%d-%m-%Y").txt'
    )
    # send_email_task = EmailOperator(
    #     task_id='send_email_task',
    #     to='anaastaasiyaaaaaa@gmail.com',
    #     subject='Airflow Alert',
    #     html_content='<p>Your Airflow job has finished.</p>',
    #     conn_id='smtp_default',
    # )

    create_file_task >> update_file_task >> clear_data_task
    
