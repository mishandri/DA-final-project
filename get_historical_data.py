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
    logging.info(f'Успешное подключения е БД.')

    # Выполняем основную программу

    api_url = "http://final-project.simulative.ru/data"

    data = pd.DataFrame()
    start_date = datetime.strptime("2022-01-01", "%Y-%m-%d")
    end_date = datetime.strptime("2022-01-02", "%Y-%m-%d")# datetime.today()

    # Создадим генератор дат для цикла for
    def date_range(start_date, end_date):
        current_date = start_date
        # Не будем включать правый конец
        while current_date < end_date:
            yield current_date
            current_date += timedelta(days=1)

    historical_data = pd.DataFrame()
    for date in date_range(start_date, end_date):
        date_str = date.strftime("%Y-%m-%d")
        new_data = fd.fetch_data(api_url, date_str)
        historical_data = pd.concat([historical_data, new_data], ignore_index=True) # Добавляем в датасет новый день
        logging.info(f'Данные за {date_str} были добавлены в датасет')

    # Если нужно сохранить данные в csv, просто раскомментируй строки ниже
    #historical_data.to_csv('historical.csv', encoding='utf8', index=False) 

    # Загрузим данные в PostgreSQL прямо из датасета

    for i in range(historical_data.shape[0]):
        print(historical_data.iloc[i]['client_id'])

except Exception as err:
    logging.error(f'Ошибка подключения к БД. {err}')
