from etl_facade import ETLFacade
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
