from functools import wraps
from typing import Callable, Optional, Any
from config.config import logger
from modules.notifications import NotificationSubject, TelegramObserver
"""
Модуль декораторов для обработки ошибок и уведомлений в ETL-процессах.

Реализует паттерн "цепочка обязанностей" для гибкой обработки ошибок:
- Повторные попытки выполнения функции (RetryDecorator)
- Логирование ошибок (LoggingDecorator)
- Отправка уведомлений (NotificationDecorator)

Содержит фабрику error_handler для удобного применения всей цепочки через один декоратор.

Классы
------
ErrorHandlerDecorator
    Базовый класс для построения цепочек декораторов обработки ошибок.
RetryDecorator
    Декоратор для повторных попыток выполнения функции при ошибках.
LoggingDecorator
    Декоратор для логирования ошибок с возможностью задания сообщения.
NotificationDecorator
    Декоратор для отправки уведомлений (например, в Telegram) при ошибках.

Функции
-------
error_handler(log_message=None, notify=True, retries=0)
    Фабрика для создания цепочки декораторов с нужными параметрами.

Пример использования
--------------------
@error_handler(log_message="Ошибка загрузки", notify=True, retries=2)
def my_func(...):
    ...
"""


class ErrorHandlerDecorator:
    """Базовый класс для декораторов обработки ошибок"""
    def __init__(self, component: Optional['ErrorHandlerDecorator'] = None):
        self._component = component

    def handle(self, func: Callable) -> Callable:
        if self._component:
            return self._component.handle(func)
        return func


class RetryDecorator(ErrorHandlerDecorator):
    """Декоратор для повторных попыток выполнения"""
    def __init__(self,
                 retries: int = 0,
                 component: Optional[ErrorHandlerDecorator] = None):
        super().__init__(component)
        self.retries = retries

    def handle(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            for attempt in range(self.retries + 1):
                try:
                    return super(RetryDecorator, self).handle(func)(*args, **kwargs)
                except Exception:
                    if attempt == self.retries:
                        raise
            return None
        return wrapper


class LoggingDecorator(ErrorHandlerDecorator):
    """Декоратор для логирования ошибок"""
    def __init__(self,
                 log_message: Optional[str] = None,
                 component: Optional[ErrorHandlerDecorator] = None):
        super().__init__(component)
        self.log_message = log_message

    def handle(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return super(LoggingDecorator, self).handle(func)(*args, **kwargs)
            except Exception as e:
                error_msg = self.log_message or f"Error in {func.__name__}"
                error_details = f"{e.__class__.__name__}: {e}"
                logger.error(f"{error_msg}: {error_details}", exc_info=True)
                raise
        return wrapper


class NotificationDecorator(ErrorHandlerDecorator):
    """Декоратор для отправки уведомлений"""
    def __init__(self,
                 notify: bool = True,
                 component: Optional[ErrorHandlerDecorator] = None):
        super().__init__(component)
        self.notify = notify

    def handle(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return super(NotificationDecorator, self).handle(func)(*args, **kwargs)
            except Exception as e:
                if self.notify:
                    subject = NotificationSubject()
                    subject.attach(TelegramObserver())
                    subject.notify(f"Ошибка в {func.__name__}: {str(e)}")
                raise
        return wrapper


def error_handler(
    log_message: Optional[str] = None,
    notify: bool = True,
    retries: int = 0
) -> Callable:
    """Фабрика для создания цепочки декораторов"""
    def decorator(func: Callable) -> Callable:
        # Создаем цепочку декораторов
        handler: ErrorHandlerDecorator = RetryDecorator(retries)
        handler = LoggingDecorator(log_message, handler)
        handler = NotificationDecorator(notify, handler)
        return handler.handle(func)
    return decorator
