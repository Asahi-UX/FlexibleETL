from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import os
from config.config import logger


class PostgresConnector:
    """
    Менеджер подключения к PostgreSQL с использованием SQLAlchemy.

    Получает параметры подключения из переменных окружения:
    - DB_USER: имя пользователя
    - DB_PASSWORD: пароль
    - DB_HOST: хост
    - DB_PORT: порт
    - DB_NAME: название БД

    Methods
    -------
    get_engine() -> Engine
        Создаёт и возвращает SQLAlchemy Engine для подключения к PostgreSQL.

    Raises
    ------
    EnvironmentError
        Если отсутствуют обязательные переменные окружения.
    SQLAlchemyError
        При ошибках подключения к базе данных.

    Examples
    --------
    engine = PostgresConnector.get_engine()
    with engine.connect() as conn:
         conn.execute(text("SELECT 1"))
    """
    @staticmethod
    def get_engine() -> Engine:
        required_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME']
        if missing := [var for var in required_vars if not os.getenv(var)]:
            raise EnvironmentError(f"Не заданы переменные окружения:{', '.join(missing)}")
        try:
            return create_engine(
                f"postgresql+psycopg2://{os.getenv('DB_USER')}"
                f":{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}"
                f":{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            )
        except SQLAlchemyError as e:  # Ловим только ошибки SQLAlchemy
            logger.error(f"Ошибка подключения к БД: {e}")
            raise
