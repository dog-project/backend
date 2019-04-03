from os import getenv
from psycopg2 import OperationalError, connect
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
        return __connect(f'/cloudsql/{INSTANCE_CONNECTION_NAME}')
    except OperationalError:
        # If production settings fail, use local development ones
        return __connect('localhost')


def __connect(host):
    """
    Helper functions to connect to Postgres
    """
    pg_config['host'] = host
    return SimpleConnectionPool(1, 1, **pg_config)

def get_connection(host="localhost"):
    pg_config["host"] = host
    return connect(**pg_config)