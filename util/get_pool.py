from os import getenv
from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool

CONNECTION_NAME = getenv('INSTANCE_CONNECTION_NAME', "")

DB_USER = getenv('POSTGRES_USER', "")
DB_PASSWORD = getenv('POSTGRES_PASSWORD', "")
DB_NAME = getenv('POSTGRES_DATABASE', "postgres")

pg_config = {
    'user': DB_USER,
    'password': DB_PASSWORD,
    'dbname': DB_NAME
}


def get_pool():
    try:
        return __connect(f'/cloudsql/{CONNECTION_NAME}')
    except OperationalError:
        # If production settings fail, use local development ones
        return __connect('localhost')

def __connect(host):
    """
    Helper functions to connect to Postgres
    """
    pg_config['host'] = host
    return SimpleConnectionPool(1, 1, **pg_config)
