# etl_facade.py

from config.config import logger
from etl.etl_registry import ETL_MAP


class ETLFacade:
    """
    Фасад для упрощённой настройки и выполнения ETL-процессов.

    Служит единой точкой входа для создания и запуска ETL-пайплайнов
    с автоматическим выбором реализации через реестр ETL_MAP.

    Parameters
    ----------
    etl_type : str
        Ключ типа ETL-процесса из реестра ETL_MAP (например, 'postgres').
    **kwargs
        Параметры для конкретного ETL-класса:
        - sql_path/sql_text: путь к SQL-файлу или текст запроса
        - export_format: формат экспорта (csv, xlsx и др.)
        - compression: настройки сжатия
        - chunk_size: размер чанков для обработки
        - send_email/send_telegram: флаги уведомлений
        - container: DI-контейнер с зависимостями

    Raises
    ------
    ValueError
        Если передан неизвестный etl_type, отсутствующий в ETL_MAP.

    Examples
    --------
    facade = ETLFacade(etl_type='postgres', sql_path='query.sql')
    facade.run_etl()
    """
    def __init__(self, etl_type: str, **kwargs):
        etl_class = ETL_MAP.get(etl_type)
        if not etl_class:
            raise ValueError(f"Неизвестный тип ETL: {etl_type}")
        self.etl = etl_class(etl_type=etl_type, **kwargs)
        self.logger = logger

    def run_etl(self):
        try:
            self.logger.info("Запуск ETL через фасад")
            self.etl.run()
            export_file = getattr(self.etl, "filename", None)
            self.logger.info(f"Экспорт завершен: {export_file}")
            return export_file
        except Exception as e:
            self.logger.error(f"Ошибка в фасаде ETL: {str(e)}")
            raise
