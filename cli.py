import argparse
from config.config import get_etl_config
from etl_registry import ETL_MAP
from modules.containers import Container
from modules.decorators import error_handler
from config.config import logger


@error_handler(notify=True, log_message="Ошибка выполнения CLI")
def main():
    """
    Основная функция CLI для запуска ETL-процессов.

    Позволяет настраивать параметры экспорта
    данных через аргументы командной строки.
    Поддерживает различные форматы экспорта,
    chunk processing и отправку уведомлений.

    Аргументы командной строки
    ----------------------
    -t, --type : str, optional
        Тип источника данных/ETL-процесса (по умолчанию: postgres).
        Значения определяются ключами ETL_MAP.
    -f, --format : str, optional
        Формат экспорта (по умолчанию: csv)
    -cpr, --compression : str, optional
        Тип сжатия для экспорта (по умолчанию из конфига)
    -q, --query : str, optional
        Путь к SQL-файлу с запросом (по умолчанию из конфига)
    --sql : str, optional
        Текст SQL-запроса для выполнения (имеет приоритет над --query).
        Пример: --sql "SELECT * FROM sales"
    -o, --output : str, optional
        Папка для экспорта данных (по умолчанию из config.ini).
    -ch, --chunk-size : int, optional
        Размер чанка для обработки данных (по умолчанию из конфига).
    --send-email : flag
        Флаг для отправки email с результатом экспорта
    --send-telegram : flag
        Отправить уведомление о завершении экспорта в Telegram.

    Поддерживаемые значения:
        - None / "none" : без сжатия (по умолчанию для xlsx)
        - "gzip"        : GZIP-сжатие (создаёт .gz файл)
        - "bz2"         : BZ2-сжатие (создаёт .bz2 файл)
        - "zip"         : ZIP-архив (создаёт .zip файл)
        - "xz"          : XZ-сжатие (создаёт .xz файл)
        - "tar"         : TAR-архив (создаёт .tar файл)
        - "zstd"        : Zstandard-сжатие (создаёт .zst файл)
        - "infer"       : автоматически определить по расширению файла

    Особенности для форматов:
    - CSV и JSON: поддерживают все указанные типы сжатия.
    Пример: compression="gzip" → файл data.csv.gz или data.json.gz.
    - XLSX: сжатие не поддерживается, параметр compression должен быть None.
    - Если указано неподдерживаемое значение, будет
      выброшено исключение ValueError.
    --------
    Примеры использования:
        python cli.py --format csv --query sql/queries.sql --compression='gzip' --send-telegram
        python cli.py -t database -ch 10000 --send-email -o export --sql "SELECT * FROM sales"
    """
    config = get_etl_config()

    parser = argparse.ArgumentParser(description="Экспорт данных из БД")
    parser.add_argument(
        "-t", "--type",
        default="postgres",
        help="Тип источника данных"
    )
    parser.add_argument(
        "-q", "--query",
        default=config['sql_path'],
        help="Путь к SQL-файлу для выборки данных"
    )
    parser.add_argument(
        "--sql",
        default=None,
        help="SQL-запрос для выполнения (альтернатива --query)"
    )
    parser.add_argument(
        "-f", "--format",
        choices=config['allowed_formats'],
        default="csv",
        help="Формат экспорта (csv, xlsx, json, parquet)"
    )
    parser.add_argument(
        "-cpr", "--compression",
        choices=['infer', None, 'bz2', 'gzip', 'tar', 'xz', 'zip', 'zstd'],
        default=config.get('compression'),
        help="Сжатие выходного файла"
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Папка для экспорта данных (по умолчанию из config.ini)'
    )
    parser.add_argument(
        "-ch", "--chunk-size",
        type=int,
        default=config.get('chunk_size'),
        help="Размер чанка для обработки"
    )
    parser.add_argument(
        "--send-email",
        action="store_true",
        help="Отправить файл на email после экспорта"
    )
    parser.add_argument(
        '--send-telegram',
        action='store_true',
        help='Отправлять уведомление о завершении экспорта в Telegram'
    )

    args = parser.parse_args()

    etl_class = ETL_MAP.get(args.type)
    if not etl_class:
        raise ValueError(f"Неизвестный тип ETL: {args.type}")
    try:
        logger.info("Запуск ETL через CLI")
        etl = etl_class(
            container=Container(),
            etl_type=args.type,
            type=args.type,
            sql_path=args.query,
            sql_text=args.sql,
            export_format=args.format,
            compression=args.compression,
            export_path=args.output,
            chunk_size=args.chunk_size,
            send_email=args.send_email,
            send_telegram=args.send_telegram
        )
        etl.run()
        # Получаем путь к файлу экспорта (он сохраняется в etl.filename)
        export_file = getattr(etl, "filename", None)
        logger.info(f"Экспорт завершен: {export_file}")

    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()
