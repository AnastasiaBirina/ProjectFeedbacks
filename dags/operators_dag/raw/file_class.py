from datetime import datetime
from pydantic import BaseModel
import logging
import shutil
import os

logger = logging.getLogger(__name__)

class FileProcessor():

    def __init__(self, original_source, updated_source):
        self.original_source = original_source
        self.updated_source = updated_source

    def create_file(self, file_name, content):
        logger.info(f"CREATE FILE {self.original_source}{file_name}")
        f = open(self.original_source + file_name, 'w+') 
        f.write(content)
        f.close()
        logger.info(f"FILE {self.original_source}{file_name} HAS BEEN CREATED")

    def update_file(self, file_name, content):
        logger.info(f"PROCESSING FILE: {file_name}")

        source_file = self.original_source + file_name
        destination_folder = self.updated_source 
        copy_file = os.path.join(destination_folder, file_name)

        # Проверяем, что файл существует
        if not os.path.exists(source_file):
            logger.error(f"FILE {source_file} DOES NOT EXIST")
            raise FileNotFoundError(f"FILE {source_file} DOES NOT EXIST")

        # Создаем папку, если её нет
        os.makedirs(destination_folder, exist_ok=True)

        logger.info(f"COPYING FILE FROM {source_file} TO {copy_file}")
        shutil.copy(source_file, copy_file)

        logger.info("APPENDING DATA")
        with open(copy_file, "a") as file:
            file.write(f"{content}")
        logger.info("FILE HAS BEEN UPDATED")

    def delete_file(self, file_name):
        source_file = self.original_source + file_name
        logger.info(f"DELETING FILE {source_file}")
        if os.path.exists(source_file):
            os.remove(source_file)
        logger.info("FILE HAS BEEN DELETED")
