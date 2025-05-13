from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator, Union, Optional, Callable
import pandas as pd
from config.config import logger, normalize_compression


class SaverStrategy(ABC):
    """
Модуль стратегий сохранения данных в различных форматах.

Реализует паттерн Strategy для экспорта pandas DataFrame или их итераторов
в форматы CSV, Excel, JSON и Parquet с поддержкой обработки чанками и сжатия.
Выбор стратегии осуществляется через SaverStrategyFactory по ключу формата.

Классы
------
SaverStrategy : ABC
    Абстрактный базовый класс для всех стратегий сохранения.
CsvSaver
    Сохраняет данные в CSV с поддержкой чанков и сжатия.
ExcelSaver
    Сохраняет данные в Excel (xlsx), без поддержки сжатия.
JsonSaver
    Сохраняет данные в JSON, поддерживает сжатие.
ParquetSaver
    Сохраняет данные в Parquet-формат, поддерживает сжатие.

Примечания
----------
- Все стратегии поддерживают обработку данных чанками.
- Для Excel-сохранения сжатие не поддерживается (будет выброшено исключение).
- Для пустых данных выводится предупреждение в логах.
"""
    @staticmethod
    def _iter_chunks(data):
        if isinstance(data, pd.DataFrame):
            yield data
        else:
            yield from data

    @abstractmethod
    def save(self,
             data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
             filepath: Path,
             compression: Optional[str] = None,
             metrics_callback: Optional[Callable[[int], None]] = None):
        pass


class CsvSaver(SaverStrategy):
    def save(self, data, filepath, compression=None, metrics_callback: Optional[Callable] = None) -> int:
        total_rows = 0
        mode = 'w'
        header = True

        compression = normalize_compression(compression)

        filepath.unlink(missing_ok=True)
        if compression == 'infer':
            compression = None
        for i, chunk in enumerate(self._iter_chunks(data)):
            if chunk.empty:
                logger.debug("Пропуск пустого чанка")
                continue

            chunk.to_csv(
                path_or_buf=filepath,
                mode=mode,
                header=header,
                index=False,
                encoding='utf-8-sig',
                compression=compression
            )

            if metrics_callback:
                metrics_callback(len(chunk))

            total_rows += len(chunk)
            mode = 'a'
            header = False
            logger.info(f"Успешно записан чанк из {len(chunk)} строк")

        if total_rows == 0:
            logger.warning("Все чанки пусты")
        return total_rows


class ExcelSaver(SaverStrategy):
    def save(self, data, filepath, compression=None, metrics_callback: Optional[Callable] = None) -> int:
        compression = normalize_compression(compression)
        if compression and compression != 'infer':
            raise ValueError("Excel format doesn't support compression")
        total_rows = 0
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            startrow = 0
            for chunk in self._iter_chunks(data):
                if chunk.empty:
                    logger.debug("Пропуск пустого чанка")
                    continue

                chunk.to_excel(
                    writer,
                    sheet_name='Data',
                    index=False,
                    startrow=startrow
                )

                if metrics_callback:
                    metrics_callback(len(chunk))

                startrow += len(chunk)
                total_rows += len(chunk)

                logger.info(f"Успешно записан чанк из {len(chunk)} строк")

        return total_rows


class JsonSaver(SaverStrategy):
    def save(self, data, filepath, compression=None, metrics_callback: Optional[Callable] = None) -> int:
        compression = normalize_compression(compression)
        total_rows = 0
        mode = 'w'
        for chunk in self._iter_chunks(data):
            if chunk.empty:
                continue
            chunk.to_json(
                filepath,
                orient='records',
                lines=True,
                force_ascii=False,
                compression=compression,
                mode=mode,
                index=False,
                # header=False if mode == 'a' else True
            )
            if metrics_callback:
                metrics_callback(len(chunk))
            total_rows += len(chunk)
            mode = 'a'  # После первого чанка - дозапись
        if total_rows == 0:
            logger.warning("Все чанки пусты")
        return total_rows


class ParquetSaver(SaverStrategy):
    def save(self, data: Union[pd.DataFrame, Iterator[pd.DataFrame]], filepath: Path,
             compression: Optional[str] = None, metrics_callback: Optional[Callable] = None) -> int:
        total_rows = 0
        for chunk in self._iter_chunks(data):
            if chunk.empty:
                continue
            chunk.to_parquet(filepath, compression=compression, index=False)
            if metrics_callback:
                metrics_callback(len(chunk))
            total_rows += len(chunk)
        return total_rows
