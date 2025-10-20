# Аналитический дашборд для маркетплейса

## Описание проекта

Проект реализует полный цикл автоматизированного сбора данных по API, их загрузку в PostgreSQL-базу и визуализацию ключевых метрик через Yandex DataLens.

Цель - обеспечить стабильный поток данных для аналитики клиентской активности, продаж и ассортиментной матрицы.

## Структура проекта

```
da-final-project/
├── .venv/			# Виртуальное окружение (скрыто)
├── python/
│   ├── config.ini		# конфиг подключения к БД (скрыт)
│   ├── fetch_data.py           # сбор информации из API и формирование датасета
│   ├── get_daily_data.py       # ежедневный сбор данных и их загрузка в PostgreSQL
│   ├── get_historical_data.py  # единоразвый сбор данных с 2022-01-01 и их загрузка в PostgreSQL
│   ├── pgdb.py         	# класс-синглтон для кодключения к БД
│   └── requirements.txt	# файл для установки всех необходимых библиотек
├── logs/
│   └── YYYY-MM-DD.log          # логи работы
├── sh/
│   └── get_data.sh          	# sh-скрипт для выполнения программы на python через .venv
├── sql/
│   ├── create_table.sql	# SQL-запросы для PostgreSQL для создания необходимой таблицы и ограничений для неё
│   └── datalens.sql            # Дополнительные SQL-запросы (с параметрами) для формирования датасетов в Yandex DataLens
├── .gitignore
└── readme.md                   # документация
```

## Автоматизация

1. Загрузка исторических данных выполнена единоразово через `get_historical_data.py`.
2. На сервер настроены следующие ежедневные задачи через cron:

   - В 6:50 удаляюся логи старше 21 дня из папки `logs`, чтобы избеать переполнения хранилища:

     ```cron
     50 6 * * * find /home/projects/final_project/logs/ -type f -mtime +21 -delete
     ```
   - В 7:00 запускается sh-скрипт, который запускает python-файл для сбора вчерашних данных и загрузки их в PostgreSQL:

     ```cron
     0 7 * * * /home/projects/final_project/get_data.sh
     ```

     Все задачи выполняются по московскому времени. Логика построена так, чтобы данные были загружены до начала рабочего дня

# Доступ к БД

- Тип: PostgreSQL
- IP: 62.113.105.120
- Порт: 5432
- База: postgres
- Пользователь: user_ro
- Пароль: user
- Права: SELECT (только чтение)

# Ссылки

[Дашборд в Yandex.DataLens](https://datalens.yandex/6eiukr5xyukyr)      |       [Исследование продаж 2023 года](DataAnalysis2023.pdf)

# Примечения

- Корректность данных и их заполнения производится на уровне PostgreSQL:
  ```pgsql
  ALTER TABLE project.project_data 
  ADD CONSTRAINT valid_discount CHECK (discount_per_item <= price_per_item),
  ADD CONSTRAINT valid_total_price CHECK (total_price = (price_per_item - discount_per_item) * quantity);
  ```
- Сервер слабенький, всего 1 процессор и 2ГБ ОЗУ, поэтому некоторые чарты могут грузиться долго.
- В целях экономии ресурсов, в самом DataLens выставлены настройки обновления кэша 1 раз в сутки + выставлена загрузка максимум 3-х чартов за раз.
- Дополнительно, для ускорения выполнения запросов, были построены индексы для:
  - client_id
  - product_id
  - purchase_datetime
  - (client_id, product_id)
  ```pgsql
    CREATE INDEX idx_project_data_client_id ON project.project_data(client_id);
    CREATE INDEX idx_project_data_product_id ON project.project_data(product_id);
    CREATE INDEX idx_project_data_purchase_datetime ON project.project_data(purchase_datetime);
    CREATE INDEX idx_project_data_client_product ON project.project_data(client_id, product_id);
  ```

- У некоторых чартов есть символ `?`, который даёт некоторые пояснения. Кроме того, некоторые элементы чартов при наведении курсора мышки выдают всплывающие подсказки.
- На каждой вкладке дашборда есть описание и цель вкладки, после прочтения можно свернуть.
- Сверху каждой вкладки закреплены селекторы, которые позволяют выбрать период и/или масштаб исследования.

**Исследование на основе данных 2023 года было решено провести прямо в DataLens, чтобы показать *самодостаточность созданного дашборда* для полноценного анализа ситуации и принятия решений.**
