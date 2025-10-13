from datetime import date, datetime, timedelta
import pandas as pd
import fetch_data as fd

api_url = "http://final-project.simulative.ru/data"

data = pd.DataFrame()
start_date = datetime.strptime("2022-01-01", "%Y-%m-%d")
end_date = datetime.strptime("2022-01-07", "%Y-%m-%d")# datetime.today()

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
historical_data.to_csv('historical.csv', encoding='utf8', index=False) # сохраняем в csv
