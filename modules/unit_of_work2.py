# infrastructure/persistence/unit_of_work.py
import os
from abc import ABC, abstractmethod
# from pathlib import Path
from typing import Any
import psycopg2
from dependency_injector import containers, providers


# 1. Конфигурация
class PostgresConfig:
    def __init__(self, host: str, port: int, user: str, password: str, dbname: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname


class ConfigFactory:
    @staticmethod
    def from_env() -> PostgresConfig:
        return PostgresConfig(
            host=os.getenv('PG_HOST', 'localhost'),
            port=int(os.getenv('PG_PORT', '5432')),
            user=os.getenv('PG_USER', 'postgres'),
            password=os.getenv('PG_PASSWORD', ''),
            dbname=os.getenv('PG_DBNAME', 'app_db')
        )


# 2. Подключение к БД
class PostgresConnector:
    def __init__(self, config: PostgresConfig):
        self.config = config
        self._connection = None

    def connect(self):
        if not self._connection or self._connection.closed:
            self._connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                dbname=self.config.dbname
            )
        return self._connection


# 3. Абстракции UoW
class AbstractUnitOfWork(ABC):
    @abstractmethod
    def __enter__(self) -> 'AbstractUnitOfWork':
        pass

    @abstractmethod
    def __exit__(self, *args):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def rollback(self):
        pass

    @property
    @abstractmethod
    def batches(self):
        pass


# 4. Реализации UoW
class PostgresUnitOfWork(AbstractUnitOfWork):
    def __init__(self, connector: PostgresConnector):
        self.connector = connector
        self.connection = None

    def __enter__(self):
        self.connection = self.connector.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        self.connection.close()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    @property
    def batches(self):
        return PostgresBatchRepository(self.connection)


class GenericUnitOfWork(AbstractUnitOfWork):
    def __init__(self, session_factory: Any):
        self.session_factory = session_factory
        self.session = None

    def __enter__(self):
        self.session = self.session_factory()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        self.session.close()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    @property
    def batches(self):
        return GenericBatchRepository(self.session)


# 5. DI-контейнер
class Container(containers.DeclarativeContainer):
    config = providers.Singleton(ConfigFactory.from_env)
    connector = providers.Singleton(
        PostgresConnector,
        config=config
    )
    uow = providers.Factory(
        PostgresUnitOfWork,
        connector=connector
    )


# 6. Пример репозиториев (для полноты реализации)
class PostgresBatchRepository:
    def __init__(self, connection):
        self.connection = connection

    def list(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT * FROM batches")
            return cursor.fetchall()


class GenericBatchRepository:
    def __init__(self, session):
        self.session = session

    def list(self):
        return self.session.execute("SELECT * FROM batches").fetchall()


# Пример использования
if __name__ == "__main__":
    container = Container()

    with container.uow() as uow:
        batches = uow.batches.list()
        print(f"Retrieved {len(batches)} batches")
        uow.commit()
