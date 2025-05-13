# di_container.py
from dependency_injector import providers
from modules.containers import Container  # Импорт из нового модуля
from etl.etl_postgres import PostgresETL


class ExtendedContainer(Container):
    """
    DI-контейнер для регистрации ETL-компонентов.

    Расширяет базовый контейнер зависимостей, добавляя фабрики для ETL-классов,
    что позволяет удобно создавать экземпляры ETL с необходимыми зависимостями.

    Attributes
    ----------
    postgres_etl : providers.Factory
        Фабрика для создания экземпляров PostgresETL.
    # mysql_etl : providers.Factory
    # Фабрика для создания экземпляров MySQLETL (закомментировано).

    Examples
    --------
    container = ExtendedContainer()
    etl = container.postgres_etl()
    """
    postgres_etl = providers.Factory(PostgresETL)
    # mysql_etl = providers.Factory(MySQLETL)
