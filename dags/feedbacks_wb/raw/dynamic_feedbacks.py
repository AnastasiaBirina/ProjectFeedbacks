import psycopg2
from datetime import date, timedelta
from airflow.hooks.base import BaseHook
import logging


log = logging.getLogger(__name__)


class DynamicFeedbacksLoader:
    """
    Ежедневная запись количества новых отзывов, полученное за сутки по каждому артикулу, в таблицу dynamic_feedbacks
    Аргументы:
        - (ds) дата запуска таски
    """

    def __init__(self, ds):
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
        Удаление данных из public.dynamic_feedbacks в случае,
        если запускаем таску на конкретную дату повторно
        """
        try:
            self.cursor.execute(
                f"""
                DELETE FROM public.dynamic_feedbacks
                WHERE date = %s
                """,
                (self.ds,),
            )
            self.conn.commit()
            log.info(f"Данные public.dynamic_feedbacks за {self.ds} успешно удалены")
        except Exception as err:
            self.conn.rollback()
            log.info(
                f"Данные public.dynamic_feedbacks за {self.ds} не удалены, ошибка: {err=}, {type(err)=}"
            )

    def load_data(self):
        """
        Запись данных в таблицу public.dynamic_feedbacks
        """
        with open("dags/feedbacks_wb/sql/dynamic_feedbacks.sql", "r") as f:
            query = f.read()

        try:
            self.cursor.execute(query, (self.ds, self.ds - timedelta(days=1)))
            self.conn.commit()
            log.info(f"Данные public.dynamic_feedbacks за {self.ds} успешно загружены")
        except Exception as err:
            self.conn.rollback()
            log.info(
                f"Данные public.dynamic_feedbacks за {self.ds} не загружены, ошибка на этапе транзакции: {err=}, {type(err)=}"
            )

        self.cursor.close()
        self.conn.close()
