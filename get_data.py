from datetime import date, timedelta
import pandas as pd
import requests
import json

api_url = "http://final-project.simulative.ru/data"
params = {
    "date":"2025-10-12"
}

data = requests.get(api_url, params)

df = pd.DataFrame(data.json())     # Считываем данные
df = df.rename(columns={'purchase_datetime': 'purchase_dt'})    # Переименуем колонку, чтобы потом её удалить
df['purchase_datetime'] = pd.to_datetime(df['purchase_dt']) + \
    pd.to_timedelta(df['purchase_time_as_seconds_from_midnight'], unit='s') # Преобразуем две колонки с датой и временем в одну
df = df.drop(['purchase_dt', 'purchase_time_as_seconds_from_midnight'], axis=1) # Удаляем старые колонки с датой и временем
print(df)


