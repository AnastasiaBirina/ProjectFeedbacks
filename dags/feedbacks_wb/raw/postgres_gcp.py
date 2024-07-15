import psycopg2
from datetime import date, timedelta
from airflow.hooks.base import BaseHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import logging
import gspread

log = logging.getLogger(__name__)


class GoogleSheetsLoader:
    """
    Получение данных из count_feedbacks и заливка их в dynamic_feedbacks
    Аргументы:
        - (ds) дата запуска таски
    """

    def __init__(self, ds):
        # GCP connection
        self.hook = GoogleBaseHook(gcp_conn_id="GOOGLE_CONNECTION")
        self.credentials = self.hook.get_credentials()
        self.google_credentials = gspread.Client(auth=self.credentials)
        self.sheet = self.google_credentials.open("WB_feedbacks_top_7")
        self.worksheet = self.sheet.worksheet("WB_feedbacks_top_7")
        self.cells = "A2:C8"

        # POSTGRES connection
        self.connection = BaseHook.get_connection("POSTGRES_CONNECTION")
        self.conn = psycopg2.connect(
            dbname=self.connection.schema,
            user=self.connection.login,
            password=self.connection.password,
            host=self.connection.host,
        )
        self.cursor = self.conn.cursor()
        self.ds = ds

    def delete_data(self):
        """
        Удаление старых данных из Google Sheets
        """
        cells = self.worksheet.range(self.cells)
        for cell in cells:
            cell.value = ""
        self.worksheet.update_cells(cells)

    def get_data(self):
        """
        Получение данных:
        - за последние 7 дней
        - id, у которых за последние 7 дней новых отзывов больше всего
        """
        with open("dags/feedbacks_wb/sql/top7_feedbacks.sql", "r") as f:
            query = f.read()
        try:
            self.cursor.execute(query, (self.ds,))
            log.info(f"Данные для дашборда на {self.ds} получены")
            return self.cursor.fetchall()
        except Exception as err:
            self.conn.rollback()
            log.info(
                f"Данные для дашборда на {self.ds} не получены, ошибка на этапе транзакции: {err=}, {type(err)=}"
            )

    def insert_data(self, data):
        """
        Запись данных в Google Sheets
        """
        cell_list = self.worksheet.range(self.cells)
        for i, row in enumerate(data):
            for j, value in enumerate(row):
                cell_list[i * 3 + j].value = str(value)
        self.worksheet.update_cells(cell_list, value_input_option="USER_ENTERED")
