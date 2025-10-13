from datetime import date, datetime, timedelta
import pandas as pd
import fetch_data as fd

api_url = "http://final-project.simulative.ru/data"

data = pd.DataFrame()
start_date = datetime.strptime("2023-01-01", "%Y-%m-%d")
end_date = datetime.today()
print(start_date, end_date)

# Создадим генератор дат для цикла for
def date_range(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)

for date in date_range(start_date, end_date):
    print(date.strftime("%Y-%m-%d"))

