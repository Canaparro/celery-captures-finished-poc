from psycopg_pool import ConnectionPool

PG_CONNINFO = "postgresql://celery:celery@localhost:5442/celery_viewer"

PG_POOL = ConnectionPool(
    conninfo=PG_CONNINFO,
    min_size=1,
    max_size=4,
    kwargs={"autocommit": True},
    open=False,
)
