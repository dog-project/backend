from os import getenv
from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool

INSTANCE_CONNECTION_NAME = getenv('INSTANCE_CONNECTION_NAME', "")

POSTGRES_USER = getenv('POSTGRES_USER', "")
POSTGRES_PASSWORD = getenv('POSTGRES_PASSWORD', "")
POSTGRES_NAME = getenv('POSTGRES_DATABASE', "postgres")

pg_config = {
    'user': POSTGRES_USER,
    'password': POSTGRES_PASSWORD,
    'dbname': POSTGRES_NAME
}


def get_pool():
    try:
        return __connect(f'/cloudsql/{INSTANCE_CONNECTION_NAME}', 5)
    except OperationalError:
        # If production settings fail, use local development ones
        return __connect('localhost', 3)


def __connect(host, max_connections):
    """
    Helper functions to connect to Postgres
    """
    pg_config['host'] = host
    return SimpleConnectionPool(1, max_connections, **pg_config)
