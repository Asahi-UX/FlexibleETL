# containers.py
from dependency_injector import containers, providers
from strategies.saver_strategy import CsvSaver, ExcelSaver, JsonSaver, ParquetSaver


class Container(containers.DeclarativeContainer):
    """
DI-контейнер для управления стратегиями сохранения данных.

Реализует регистрацию и фабричное создание стратегий экспорта данных
в различные форматы (CSV, Excel, JSON, Parquet) через dependency-injector.

Attributes
----------
csv_saver : providers.Factory
    Фабрика для создания экземпляров CsvSaver
xlsx_saver : providers.Factory
    Фабрика для создания экземпляров ExcelSaver
json_saver : providers.Factory
    Фабрика для создания экземпляров JsonSaver
parquet_saver : providers.Factory
    Фабрика для создания экземпляров ParquetSaver

saver_strategy_map : providers.Dict
    Словарь для сопоставления форматов (csv, xlsx и др.) с фабриками стратегий
saver_factory : providers.Callable
    Фабричный метод для получения стратегии по ключу формата

Examples
--------
container = Container()
strategy = container.saver_factory('csv')

Notes
-----
Для добавления новой стратегии:
1. Зарегистрируйте фабрику стратегии
2. Добавьте запись в saver_strategy_map
"""
    # Регистрация стратегий сохранения
    csv_saver = providers.Factory(CsvSaver)
    xlsx_saver = providers.Factory(ExcelSaver)
    json_saver = providers.Factory(JsonSaver)
    parquet_saver = providers.Factory(ParquetSaver)

    # Словарь стратегий
    saver_strategy_map = providers.Dict({
        'csv': csv_saver,
        'xlsx': xlsx_saver,
        'json': json_saver,
        'parquet': parquet_saver
    })

    saver_factory = providers.Callable(lambda strategy_map,
                                       fmt: strategy_map[fmt](),
                                       strategy_map=saver_strategy_map)
