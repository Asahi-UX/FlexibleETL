from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from config.db_connectors import PostgresConnector
from config.config import logger


class UnitOfWork:
    """
    Менеджер единицы работы для управления транзакциями и сессиями БД.

    Обеспечивает паттерн Unit of Work для атомарных операций с PostgreSQL через SQLAlchemy,
    автоматически обрабатывает коммит/роллбэк транзакций и закрытие сессий.

    Attributes
    ----------
    engine : Engine
        SQLAlchemy engine для подключения к PostgreSQL.
    session_factory : sessionmaker
        Фабрика для создания новых сессий БД.

    Methods
    -------
    session_scope()
        Контекстный менеджер для работы с сессией БД. Возвращает генератор сессий.

    Examples
    --------
    uow = UnitOfWork()
    with uow.session_scope() as session:
         session.execute(text("SELECT 1"))
    """
    def __init__(self):
        self.engine = PostgresConnector.get_engine()
        self.session_factory = sessionmaker(bind=self.engine,
                                            autocommit=False,
                                            autoflush=False
                                            )

    @contextmanager
    def session_scope(self):
        session = self.session_factory()
        try:
            logger.debug("Начало транзакции")
            yield session
            session.commit()
            logger.debug("Коммит транзакции")
        except Exception as e:
            session.rollback()
            logger.error(f"Ошибка транзакции: {str(e)}", exc_info=True)
            raise
        finally:
            session.close()
            logger.debug("Сессия закрыта")
