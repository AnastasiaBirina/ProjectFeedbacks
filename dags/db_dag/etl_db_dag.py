from datetime import datetime
from db_dag.raw.db_ import DatabaseHandler
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="db_dag",
    schedule="0 8 * * *",
    start_date=datetime(2025, 3, 22),
    catchup=False,
    tags=["postgres", "python"],
):

    def data_from_db_task(ds):
        ds_date = datetime.strptime(ds, "%Y-%m-%d")
        DBprocess = DatabaseHandler(ds_date)
        DBprocess.delete_data()
        DBprocess.insert_data()


    # Получение 100 первых товаров в поисковой выдаче по запросу с WB
    # и заливка этих данных в Postgres в таблицу count_feedbacks
    data_from_db = PythonOperator(
        task_id="data_from_db",
        python_callable=data_from_db_task,
        op_kwargs={"current_dag_run_date": "{{ds}}"},
    )

 
    data_from_db