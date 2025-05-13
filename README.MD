# ETL Framework with DI and Design Patterns

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://python.org)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-green)](https://sqlalchemy.org)
[![Dependency-Injector](https://img.shields.io/badge/Dependency%20Injector-4.0-red)](https://python-dependency-injector.ets-labs.org)

**Мощный ETL-фреймворк** для извлечения, трансформации и загрузки данных с поддержкой:
- DI-контейнеров 🧩
- Паттернов проектирования 🎨
- Многопоточности и чанков 🚀
- Уведомлений (Email/Telegram) 📧
- Сжатия данных (GZip, ZIP и др.) 📦

## 🌟 Основные возможности

- **Гибкий ETL-пайплайн** с поддержкой PostgreSQL, MySQL (можно расширить)
- **5+ стратегий экспорта**: CSV, Excel, JSON, Parquet и др.
- **Автоматическое управление зависимостями** через DI-контейнеры
- **Уведомления** о завершении задач через Observer-паттерн
- **CLI-интерфейс** для запуска из командной строки
- **Логирование** и обработка ошибок с повторными попытками

## 🛠 Технологический стек

- **Язык**: Python 3.9+
- **БД**: PostgreSQL (поддержка других через SQLAlchemy)
- **DI**: `dependency-injector`
- **Сериализация**: `pandas`, `pyarrow`
- **Логирование**: `structlog`/`logging`
- **Тестирование**: `pytest` (примеры в тестах)

## 🚀 Быстрый старт

### Установка
git clone https://github.com/yourusername/etl-framework.git
cd etl-framework
pip install -r requirements.txt

text

### Настройка .env
DB_USER=postgres
DB_PASSWORD=secret
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydb

text

### Пример через CLI
python cli.py
--type postgres
--query sql/sales_report.sql
--format csv
--compression gzip
--send-email

text

### Пример через Python API
from etl.etl_facade import ETLFacade

facade = ETLFacade(
etl_type='postgres',
sql_path='sql/sales_report.sql',
export_format='parquet',
compression='snappy'
)
facade.run_etl()

text

## 🧩 Архитектура модулей

### Основные компоненты
1. **ETLBase** - абстрактный класс для создания ETL-процессов
2. **PostgresETL** - реализация ETL для PostgreSQL
3. **UnitOfWork** - менеджер транзакций БД
4. **SaverStrategy** - стратегии экспорта данных (CSV, Excel и др.)
5. **NotificationSubject** - система уведомлений (Observer)
6. **DI-контейнеры** - управление зависимостями

### Схема работы
graph TD
CLI[CLI Аргументы] --> Facade[ETLFacade]
Facade --> ETL[PostgresETL]
ETL --> |Извлечение| DB[(PostgreSQL)]
ETL --> |Трансформация| Transform[Pandas]
ETL --> |Сохранение| Strategy[CsvSaver/ExcelSaver]
ETL --> |Уведомления| Notify[Email/Telegram]

text

## 📚 Документация

### Поддерживаемые форматы экспорта
| Формат   | Сжатие              | Чанки |
|----------|---------------------|-------|
| CSV      | GZIP, ZIP, ZSTD     | ✅    |
| Excel    | ❌                  | ✅    |
| JSON     | GZIP, BZ2, LZMA     | ✅    |
| Parquet  | SNAPPY, GZIP, BROTLI| ✅    |

### Уведомления
Добавление кастомного уведомления
from modules.notifications import NotificationSubject, Observer

class SlackObserver(Observer):
def update(self, message: str, **kwargs):
...

subject = NotificationSubject()
subject.attach(SlackObserver())

text

### Добавление нового ETL
1. Создать класс в `etl/etl_mysql.py` унаследовав от ETLBase
2. Зарегистрировать в `etl_registry.py`
etl_registry.py
from etl.etl_mysql import MySQLETL

ETL_MAP = {
'postgres': PostgresETL,
'mysql': MySQLETL # <-- Новая реализация
}

text

## 📄 Лицензия
MIT License. Подробнее в файле [LICENSE](LICENSE).

---

**Разработано с ❤️ для эффективной работы с данными**
[Совершенствуйте, расширяйте, используйте!](https://github.com/yourusername/etl-framework/issues)