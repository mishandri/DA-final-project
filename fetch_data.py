from datetime import date, datetime, timedelta
import pandas as pd
import requests
import json

# Считаем данные, которые не подходят по структуре ошибочными
def fetch_data (api_url:str, date:str)->pd.DataFrame:
    """Функция делает запрос к API по api_url и 
    возвращает датафрейм данных за дату date"""
    params = {
        "date":date
    }
    response = requests.get(api_url, params) # делаем запрос
    df = pd.DataFrame(response.json())     # Считываем данные
    df = df.rename(columns={'purchase_datetime': 'purchase_dt'})    # Переименуем колонку, чтобы потом её удалить
    df['purchase_datetime'] = pd.to_datetime(df['purchase_dt']) + \
        pd.to_timedelta(df['purchase_time_as_seconds_from_midnight'], unit='s') # Преобразуем две колонки с датой и временем в одну
    df = df.drop(['purchase_dt', 'purchase_time_as_seconds_from_midnight'], axis=1) # Удаляем старые колонки с датой и временем
    return df