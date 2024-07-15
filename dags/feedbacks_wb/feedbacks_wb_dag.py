from datetime import datetime
from feedbacks_wb.raw.wb_postgres import WBLoader
from feedbacks_wb.raw.dynamic_feedbacks import DynamicFeedbacksLoader
from feedbacks_wb.raw.postgres_gcp import GoogleSheetsLoader
from datetime import datetime, timedelta

from airflow import DAG
import logging
from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.date_time_sensor import DateTimeSensor


log = logging.getLogger(__name__)
query = "Наклейки для творчества"
url = (
    "https://search.wb.ru/exactmatch/ru/common/v4/search?TestGroup=no_test&TestID=no_test&appType=1&curr=rub&dest=-1257786&query="
    + query
    + "&resultset=catalog&sort=popular&spp=99&suppressSpellcheck=false"
)

with DAG(
    dag_id="Feedbacks_WB",
    schedule="0 19 * * *",
    start_date=datetime(2024, 4, 30),
    catchup=False,
    tags=["postgres", "python"],
):

    def wb_data_to_postgres_task(ds):
        ds_date = datetime.strptime(ds, "%Y-%m-%d")
        WB_data = WBLoader(ds_date)
        WB_data.delete_data()
        data = WB_data.wb_postgres_loader()
        WB_data.insert_data_count_feedbacks(data)

    def insert_data_dynamic_feedbacks_task(ds):
        ds_date = datetime.strptime(ds, "%Y-%m-%d")
        dynamic_feedbacks = DynamicFeedbacksLoader(ds_date)
        dynamic_feedbacks.delete_data()
        dynamic_feedbacks.load_data()

    def postgres_to_google_sheets_task(ds):
        ds_date = datetime.strptime(ds, "%Y-%m-%d")
        google_sheets_data = GoogleSheetsLoader(ds_date)
        google_sheets_data.delete_data()
        data = google_sheets_data.get_data()
        google_sheets_data.insert_data(data)

    # Получение 100 первых товаров в поисковой выдаче по запросу с WB
    # и заливка этих данных в Postgres в таблицу count_feedbacks
    wb_data_to_postgres = PythonOperator(
        task_id="wb_data_to_postgres_task",
        python_callable=wb_data_to_postgres_task,
        op_kwargs={"current_dag_run_date": "{{ds}}"},
    )

    # Пауза до 19:05
    wait_19_05 = DateTimeSensor(task_id="19_05_wait", target_time="19:05:00")

    # Ежедневная запись количества новых отзывов, полученное за сутки по каждому артикулу, в таблицу dynamic_feedbacks
    insert_data_dynamic_feedbacks = PythonOperator(
        task_id="insert_data_dynamic_feedbacks_task",
        python_callable=insert_data_dynamic_feedbacks_task,
        op_kwargs={"current_dag_run_date": "{{ds}}"},
    )

    # Пауза до 20:00
    wait_20 = DateTimeSensor(task_id="20_00_wait", target_time="20:00:00")

    # Заливка данных в Google Sheets:
    # - за последние 7 дней
    # - id, у которых за последние 7 дней новых отзывов больше всего
    # Построение графика на основе этих данных
    postgres_to_google_sheets = PythonOperator(
        task_id="postgres_to_google_sheets_task",
        python_callable=postgres_to_google_sheets_task,
        op_kwargs={"current_dag_run_date": "{{ds}}"},
    )

    (
        wb_data_to_postgres
        >> wait_19_05
        >> insert_data_dynamic_feedbacks
        >> wait_20
        >> postgres_to_google_sheets
    )
