from datetime import date, datetime, timedelta
import pandas as pd
import fetch_data as fd
import logging  # Для логирования
import configparser # Для конфигов
from pgdb import PGDatabase # Для подключения к БД

# Настройка логера:
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO,
    encoding='utf-8',
    filename=f'{dirname}/logs/{today.strftime("%Y-%m-%d")}.log',
    filemode='a'
)

# Настройка кофиг-файла
# Настройки следует хранить в файле config.ini
# [sql]
# HOST=Адрес сервера БД
# PORT=Порт сервера БД
# DATABASE=Имя БД
# USER=Пользователь
# PASSWORD=Пароль

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
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Данные за {date_str} были добавлены в датасет')

# Если нужно сохранить данные в csv, просто раскомментируй строку ниже
#historical_data.to_csv('historical.csv', encoding='utf8', index=False) 

# Загрузим данные в PostgreSQL прямо из датасета

for i in range(historical_data.shape[0]):
    print(historical_data.iloc[i]['client_id'])
