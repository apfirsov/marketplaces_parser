# fastAPI_parser
parser

Перед сборкой контейнера с БД необходимо создать в корне проета .env файл с переменными для ДБ

```DB_NAME=parser```

```POSTGRES_USER=parser```

```POSTGRES_PASSWORD=parser```

```ASYNC_DATABASE_URL=postgresql+asyncpg://user_name:password@0.0.0.0:5432/db_name```

```DATABASE_URL=postgresql://user_name:password@0.0.0.0:5432/db_name```

Для накатывания миграций, если файла alembic.ini ещё нет, нужно запустить в терминале команду:

```
alembic init migrations
```

После этого будет создана папка с миграциями и конфигурационный файл для алембика.

- В alembic.ini нужно задать адрес базы данных, в которую будем катать миграции.
- Дальше идём в папку с миграциями и открываем env.py, там вносим изменения в блок, где написано 

```
from myapp import mymodel
```

- Дальше вводим: ```alembic revision --autogenerate -m "comment"```
- Будет создана миграция
- Дальше вводим: ```alembic upgrade heads```
