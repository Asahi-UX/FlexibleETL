from modules.di_container import ExtendedContainer as Container
from modules.unit_of_work import UnitOfWork

container = Container()
uow = UnitOfWork()

etl = container.postgres_etl(
    container=container,
    uow=uow,
    sql_path='sql/queries.sql',
    export_format='csv',
    compression='gzip',
    export_path='./expo',
    chunk_size=20000,
    send_email=True,
    send_telegram=True
)

etl.run()
