import requests
import json
import psycopg2
from airflow.hooks.base import BaseHook
from bs4 import BeautifulSoup
from soup2dict import convert
from datetime import datetime
from pydantic import BaseModel


# class User(BaseModel):
#     id: str = 'John Doe'  
#     ds: datetime  
#     price: datetime | None  
#     feedbacks: dict[str, PositiveInt]

class WBLoader:
    """
    Загрузка данных из WB в таблицу public.count_feedbacks в Postgres
    Аргументы:
        - (ds) дата запуска таски
    """

    def __init__(self, ds):
        self.query = "Наклейки для творчества"
        self.url = (
            "https://search.wb.ru/exactmatch/ru/common/v4/search?TestGroup=no_test&TestID=no_test&appType=1&curr=rub&dest=-1257786&query="
            + self.query
            + "&resultset=catalog&sort=popular&spp=99&suppressSpellcheck=false"
        )
        self.connection = BaseHook.get_connection("POSTGRES_CONNECTION")
        self.conn = psycopg2.connect(
            dbname=self.connection.schema,
            user=self.connection.login,
            password=self.connection.password,
            host=self.connection.host,
        )
        self.cursor = self.conn.cursor()
        self.ds = ds
        self.tries_max = 50

    def delete_data(self):
        """
        Удаление данных из public.count_feedbacks в случае,
        если запускаем таску на конкретную дату повторно
        """
        try:
            self.cursor.execute(
                f"""
                DELETE FROM public.count_feedbacks
                WHERE date = %s
                """,
                (self.ds,),
            )
            self.conn.commit()
            log.info(
                f"Данные public.count_feedbacks за {self.ds} успешно удалены"
            )
        except Exception as err:
            self.conn.rollback()
            log.info(
                f"Данные public.count_feedbacks за {self.ds} не удалены, ошибка: {err=}, {type(err)=}"
            )

    def insert_data_count_feedbacks(self, clear_data):
        """
        Запись полученных очищенных данных в таблицу public.count_feedbacks
        Аргументы:
            - (clear_data) очищенные отзывы, преобразованные в словарь
        """
        # Запись данных за сегодня
        for product in clear_data:
            log.info(str(product["id"]) + "  ~~  " + str(self.ds))
            self.cursor.execute(
                """
                INSERT INTO count_feedbacks (date, id, salePriceU, feedbacks)
                VALUES (%s, %s, %s, %s)
                """,
                (self.ds, product["id"], product["priceU"], product["feedbacks"]),
            )

        try:
            self.conn.commit()
            log.info(f"Данные count_feedbacks за {self.ds} успешно загружены")
        except Exception as err:
            self.conn.rollback()
            log.info(
                f"Данные count_feedbacks за {self.ds} не загружены, ошибка на этапе транзакции: {err=}, {type(err)=}"
            )

        self.cursor.close()
        self.conn.close()

    def get_data_from_wb(self):
        """
        Единичное подключение и получение данных с WB.
        Здесь есть проверка: актуальные данные только в том случае,
        когда возвращается total в резуультате запроса
        """
        response = requests.get(self.url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            clear_data = json.loads(convert(soup)["navigablestring"][0])["data"]
            if "total" in clear_data.keys():
                return clear_data["products"]
            else:
                return False
        else:
            log.info(f"Ошибка: {response.status_code} - {response.text}")
            return False

    def wb_postgres_loader(self):
        """
        Основная функция получаения данных с WB и заливки из в БД
        """
        tries_cur = 0
        clear_data = self.get_data_from_wb()

        # Пытаемся забрать данные
        while not clear_data and tries_cur < self.tries_max:
            clear_data = self.get_data_from_wb()
            tries_cur += 1

        # Если данные так и не забрали, пишем об этом. Иначе идет обработка данных и запись их в БД
        log.info(f"Попыток: {tries_cur}")
        if not clear_data:
            raise ValueError(
                f"Не получилось получить данные (больше {self.tries_max} попыток)"
            )
        else:
            return clear_data
