import pandas as pd
from pathlib import Path
from sqlalchemy import text
from typing import Iterator
from config.db_connectors import PostgresConnector
from modules.decorators import error_handler
from etl.etl_base import ETLBase
from modules.unit_of_work import UnitOfWork


class PostgresETL(ETLBase):
    """
    ETL-процесс для извлечения данных из PostgreSQL с использованием SQLAlchemy и pandas.

    Поддерживает чтение SQL-запроса из строки или файла, обработку данных чанками,
    а также интеграцию с системой Unit of Work для управления сессиями.

    Methods
    -------
    extract() -> Iterator[pd.DataFrame]
        Извлекает данные из PostgreSQL по заданному SQL-запросу, возвращая итератор DataFrame.
    transform(data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]
        Возвращает данные без изменений (заглушка для трансформаций).
    """
    def __init__(self,
                 container,
                 sql_text=None,
                 **kwargs):
        super().__init__(container=container, **kwargs)
        self.engine = PostgresConnector.get_engine()
        self.sql_text = sql_text
        self.uow = UnitOfWork()

    @error_handler(log_message="Ошибка извлечения данных", notify=True)
    def extract(self) -> Iterator[pd.DataFrame]:
        if self.sql_text:
            sql_query = text(self.sql_text)
        else:
            if not Path(self.sql_path).exists():
                raise FileNotFoundError(f"SQL-файл не найден: {self.sql_path}")
            with open(self.sql_path, 'r', encoding='utf-8') as f:
                sql_query = text(f.read())
        with self.uow.session_scope() as session:
            yield from pd.read_sql(sql_query,
                                   session.connection(),
                                   chunksize=self.chunk_size,
                                   dtype_backend='pyarrow'
                                   )

    def transform(self,
                  data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        return data
