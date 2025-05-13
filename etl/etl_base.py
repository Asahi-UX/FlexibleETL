from __future__ import annotations
import pandas as pd
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator, Optional, Dict, Any, Callable
from config.config import get_etl_config, logger, normalize_compression
from modules.decorators import error_handler
from modules.notifications import NotificationSubject, EmailObserver, TelegramObserver
from datetime import datetime
import time
from memory_profiler import memory_usage  # type: ignore
from modules.containers import Container


class ETLBase(ABC):
    """
    Базовый абстрактный класс для ETL-процесса.

    Определяет общий интерфейс и логику для извлечения, трансформации и загрузки данных,
    а также поддерживает экспорт в различные форматы, сжатие, уведомления и сбор метрик.

    Methods
    -------
    extract()
        Извлекает данные из источника (реализуется в подклассах).
    transform(data)
        Преобразует данные (реализуется в подклассах).
    load(data, metrics_callback=None)
        Загружает данные в файл.
    run()
        Запускает полный ETL-процесс.
    """
    def __init__(self, container: Container, send_email=False, send_telegram=False, **kwargs) -> None:
        self.container = container
        self.config = get_etl_config()
        self.filename: Optional[Path] = None
        self.logger = logger

        self.type = kwargs.get('type', 'postgres')
        self.export_format = kwargs.get('export_format', self.config['export_format'])
        self.compression = kwargs.get('compression', self.config['compression'])
        self.exports_path = Path(kwargs.get('export_path') or self.config.get('export_path') or './exports')
        self.exports_path.mkdir(parents=True, exist_ok=True)
        self.sql_path = kwargs.get('sql_path', self.config.get('sql_path'))
        self.chunk_size = kwargs.get('chunk_size', self.config.get('chunk_size'))
        self.send_email = send_email
        self.send_telegram = send_telegram
        self.metrics: Dict[str, Any] = {
            'start_time': None,
            'memory_usage': [],
            'rows_processed': 0
            }

    def _build_filename(self) -> str:
        timestamp = datetime.now().strftime('%d.%m.%Y_%H-%M-%S')
        compression = normalize_compression(self.compression)
        ext = f".{compression}" if compression and compression != "none" else ""
        return f"export_{timestamp}.{getattr(self, 'export_format', 'csv')}{ext}"

    def generate_filename(self) -> Path:
        return self.exports_path / self._build_filename()

    @error_handler(log_message="Ошибка выполнения ETL", notify=True)
    def run(self):
        self.logger.info(
            "Параметры запуска:\n"
            f"  Тип          : {self.type}\n"
            f"  Путь к SQL   : {self.sql_path}\n"
            f"  Расширение   : {self.export_format}\n"
            f"  Сжатие       : {self.compression}\n"
            f"  Директория   : {self.exports_path}\n"
            f"  Размер чанка : {self.chunk_size}\n"
            f"  E-mail       : {self.send_email}\n"
            f"  Telegram     : {self.send_telegram}"
        )
        self.metrics['start_time'] = time.time()
        self.metrics['rows_processed'] = 0
        try:
            data = self.extract()
            transformed = self.transform(data)
            self.load(transformed)
        finally:
            self._send_report()
            self._log_metrics()

    def _log_metrics(self):
        duration = time.time() - self.metrics['start_time']
        max_mem = max(self.metrics['memory_usage']) if self.metrics['memory_usage'] else 0
        logger.info(
            f"Метрики выполнения:\n"
            f"  Время             : {duration:.2f} сек\n"
            f"  Пиковая память    : {max_mem:.2f} MiB\n"
            f"  Обработано строк  : {self.metrics['rows_processed']}"
        )

    @abstractmethod
    def extract(self) -> Iterator[pd.DataFrame]:
        pass

    @abstractmethod
    def transform(self,
                  data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        pass

    @error_handler(log_message="Ошибка загрузки данных", notify=True)
    def load(self, data: Iterator[pd.DataFrame], metrics_callback: Optional[Callable] = None) -> None:

        saver = self.container.saver_strategy_map()[self.export_format]
        self.filename = self.generate_filename()

        total_rows = saver.save(
            data=data,
            filepath=self.filename,
            compression=self.compression,
            metrics_callback=self._update_metrics
        )
        if total_rows == 0:
            raise ValueError("Нет данных для сохранения")
        self.logger.info(f"Данные сохранены в папку {self.exports_path}")

    def _update_metrics(self, rows: int):
        self.metrics['rows_processed'] += rows
        self.metrics['memory_usage'].append(memory_usage(proc=-1, interval=0.5, backend='psutil', max_usage=True))

    @error_handler(log_message="Ошибка отправки отчета", notify=True)
    def _send_report(self) -> None:
        if self.filename is None or not Path(self.filename).exists():
            return

        subject = NotificationSubject()
        if getattr(self, "send_email", False):
            subject.attach(EmailObserver())
        if getattr(self, "send_telegram", False):
            subject.attach(TelegramObserver())

        subject.notify(
            f"ETL завершен. Файл: {Path(self.filename).name}",
            subject=f"Экспорт данных от {Path(self.filename).name}",
            body=f"Файл экспорта: {self.filename.name}",
            attachment_path=str(self.filename)
        )
