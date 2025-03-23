import requests
import json
import psycopg2
from airflow.hooks.base import BaseHook
from bs4 import BeautifulSoup
from soup2dict import convert
from datetime import datetime
from pydantic import BaseModel
import logging

log = logging.getLogger(__name__)

class DatabaseHandler:
    """
    Загрузка данных в таблицу public.sales_processed в Postgres
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
        Удаление данных из public.sales_processed в случае,
        если запускаем таску на конкретную дату повторно
        """
        try:
            self.cursor.execute(
                f"""
                DELETE FROM public.sales_processed
                WHERE date = %s
                """,
                (self.ds,),
            )
            self.conn.commit()
            log.info(
                f"Данные public.sales_processed за {self.ds} успешно удалены"
            )
        except Exception as err:
            self.conn.rollback()
            log.info(
                f"Данные public.sales_processed за {self.ds} не удалены, ошибка: {err=}, {type(err)=}"
            )

    def insert_data(self):
        """
        Запись данных в таблицу public.sales_processed
        """

        self.cursor.execute(
             """
            INSERT INTO public.sales_processed 
            SELECT %s date, SUM(salepriceu)
            FROM public.sales_raw
            WHERE date = %s
                """,
                (self.ds, self.ds),
        )

        try:
            self.conn.commit()
            log.info(f"Данные sales_processed за {self.ds} успешно загружены")
        except Exception as err:
            self.conn.rollback()
            log.info(
                f"Данные sales_processed за {self.ds} не загружены, ошибка на этапе транзакции: {err=}, {type(err)=}"
            )

        self.cursor.close()
        self.conn.close()

   