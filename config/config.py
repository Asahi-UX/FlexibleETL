"""
Модуль конфигурации и инициализации логирования для ETL-проекта.

Содержит функции для:
- Загрузки и парсинга конфигурационного INI-файла
- Настройки логирования на основе файла конфигурации
- Получения соединения с базой данных PostgreSQL через SQLAlchemy
- Формирования ETL-конфига на основе INI-файла и переменных окружения

Использует переменные окружения для
чувствительных данных (логины, пароли, пути).
"""
import configparser
import logging.config
import json
import os
from pathlib import Path
from functools import lru_cache
from dotenv import load_dotenv


load_dotenv(override=True)
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / 'config.ini'
LOGS_DIR = BASE_DIR.parent / "logs"
LOGS_PATH = BASE_DIR / 'logs.json'
# ---------- Логирование ----------


def setup_logging():
    """
    Настраивает логирование для проекта
    на основе конфигурационного файла.

    Если файл логирования не найден или повреждён,
    устанавливает базовую конфигурацию.
    """
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        with open(LOGS_PATH, 'r', encoding='utf-8') as f:
            logging.config.dictConfig(json.load(f))
    except Exception as e:
        logging.basicConfig(level=logging.INFO)
        logging.getLogger().error("Ошибка загрузки лог-конфига: %s", e)


setup_logging()
logger = logging.getLogger('etl_logger')

# ---------- Конфиг ----------


@lru_cache(maxsize=None)
def load_config():
    """
    Загружает и парсит конфигурационный INI-файл.

    Returns
    -------
    configparser.ConfigParser
        Объект с загруженными настройками.
    """
    config = configparser.ConfigParser()

    if not config.read(CONFIG_PATH):
        logger.critical("Файл конфигурации не найден: %s", CONFIG_PATH)
        raise FileNotFoundError(f"Файл конфигурации не найден: {CONFIG_PATH}")
    return config


def normalize_compression(value):
    if value is None:
        return None
    if isinstance(value, str) and value.strip().lower() in ("none", "null", ""):
        return None
    return value


# ---------- ETL-конфиг ----------
def get_etl_config():
    """
    Формирует словарь ETL-конфигурации на
    основе INI-файла и переменных окружения.

    Returns
    -------
    dict
        Словарь с ключевыми параметрами ETL:
        - chunk_size: int, размер чанка для обработки данных
        - sql_path: str, путь к SQL-файлу
        - export_path: str, директория для экспорта
        - allowed_formats: list[str], разрешённые форматы экспорта
    """
    config = load_config()
    return {
        'chunk_size': config.getint('ETL',
                                    'chunk_size',
                                    fallback=10000),
        'sql_path': os.getenv('SQL_PATH',
                              config.get('ETL', 'sql_path',
                                         fallback='sql/queries.sql')),
        'export_path': os.getenv('EXPORT_PATH',
                                 config.get('ETL', 'export_path',
                                            fallback='./exports')),
        'allowed_formats': config.get('ETL', 'allowed_formats').split(','),
        'export_format': os.getenv('EXPORT_FORMAT', config.get('ETL', 'export_format',
                                                               fallback='csv')),
        'compression': os.getenv('COMPRESSION',
                                 config.get('ETL', 'compression',
                                            fallback=None))
    }
