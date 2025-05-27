from etl.etl_facade import ETLFacade
from modules.containers import Container
"""
Фасад для запуска ETL-процесса с настройкой параметров через ETLFacade.
После выполнения путь к файлу и метрики будут доступны в логах.
"""

facade = ETLFacade(
    etl_type='postgres',
    sql_path='sql/queries.sql',
    export_format='csv',
    compression=None,
    export_path='./exports',
    chunk_size=100000,
    send_email=False,
    send_telegram=False,
    container=Container()
)

facade.run_etl()


"""
- **Гибкий ETL-пайплайн** с поддержкой PostgreSQL, MySQL (можно расширить)
- **5+ стратегий экспорта**: CSV, Excel, JSON, Parquet и др.
- **Автоматическое управление зависимостями** через DI-контейнеры
- **Уведомления** о завершении задач через Observer-паттерн
- **CLI-интерфейс** для запуска из командной строки
- **Логирование** и обработка ошибок с повторными попытками"""