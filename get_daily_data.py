import os
from datetime import date, datetime, timedelta
import pandas as pd
import fetch_data as fd
import logging  # Для логирования
import configparser # Для конфигов
from pgdb import PGDatabase # Для подключения к БД

dirname = os.path.dirname(__file__)
today = datetime.today()

# Настройка кофиг-файла
config = configparser.ConfigParser()
config.read(os.path.join(dirname, "config.ini"))
PSQL = config['sql']
# Настройки следует хранить в файле config.ini
# [sql]
# HOST=Адрес сервера БД
# PORT=Порт сервера БД
# DATABASE=Имя БД
# USER=Пользователь
# PASSWORD=Пароль

# Настройка логера:
os.makedirs(os.path.join(dirname, "logs"), exist_ok=True) # Создаём папку logs, если её не существует
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO,
    encoding='utf-8',
    filename=f'{dirname}/logs/{today.strftime("%Y-%m-%d")}.log',
    filemode='a'
)
logging.info(f"Запуск скрипта {os.path.basename(__file__)} для выгрузки данных в БД...")

# Попытка подключения к БД
try:
    database = PGDatabase(
        host=PSQL["HOST"],
        port=PSQL["PORT"],
        database=PSQL["DATABASE"],
        user=PSQL["USER"],
        password=PSQL["PASSWORD"]
    )
    logging.info(f'Успешное подключения к БД.')

    # Выполняем основную программу

    api_url = "http://final-project.simulative.ru/data"

    data = pd.DataFrame()
    yesterday_date = datetime.today() - timedelta(days=1)

    date_str = yesterday_date.strftime("%Y-%m-%d")
    new_data = fd.fetch_data(api_url, date_str)
    data = pd.concat([data, new_data], ignore_index=True) # Добавляем в датасет новый день
    logging.info(f'Данные за {date_str} были добавлены в датасет')


    # Загрузим данные в PostgreSQL прямо из датасета
    for i, row in data.iterrows():
        # Сразу проверяем на типы данных, чтобы не было проблем со вставкой.
        # Некорректные данные не будут вставляться
        query = f"""INSERT INTO project.project_data 
                    (client_id, gender, product_id, quantity, price_per_item, discount_per_item, total_price, purchase_datetime)
                    VALUES (CAST('{row['client_id']}' AS INTEGER),
                            '{row['gender']}', 
                            CAST('{row['product_id']}' AS INTEGER), 
                            CAST('{row['quantity']}' AS INTEGER), 
                            CAST('{row['price_per_item']}' AS DECIMAL(10,2)), 
                            CAST('{row['discount_per_item']}' AS DECIMAL(10,2)), 
                            CAST('{row['total_price']}' AS DECIMAL(10,2)),
                            CAST('{row['purchase_datetime']}' AS TIMESTAMP)
                            )"""
        try:
            database.post(query)
        except Exception as err:
            logging.error(f'Ошибка вставки данных. {err}')

except Exception as err:
    logging.error(f'Ошибка подключения к БД. {err}')
