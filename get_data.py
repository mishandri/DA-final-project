from datetime import date, timedelta
import requests

api_url = "http://final-project.simulative.ru/data"
params = {
    "date":"2025-10-12"
}

data = requests.get(api_url, params)
print(data.json())