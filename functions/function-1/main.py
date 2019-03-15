from os import getenv

import psycopg2
from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool

CONNECTION_NAME = getenv('INSTANCE_CONNECTION_NAME', "")

DB_USER = getenv('POSTGRES_USER', "")
DB_PASSWORD = getenv('POSTGRES_PASSWORD', "")
DB_NAME = getenv('POSTGRES_DATABASE', "")

pg_config = {
  'user': DB_USER,
  'password': DB_PASSWORD,
  'dbname': DB_NAME
}

# Connection pools reuse connections between invocations,
# and handle dropped or expired connections automatically.
pg_pool = None

def __connect(host):
    """
    Helper functions to connect to Postgres
    """
    global pg_pool
    pg_config['host'] = host
    pg_pool = SimpleConnectionPool(1, 1, **pg_config)


def insert_example_table(request):
    request_json = request.get_json()
    
    global pg_pool

    # Initialize the pool lazily, in case SQL access isn't needed for this
    # GCF instance. Doing so minimizes the number of active SQL connections,
    # which helps keep your GCF instances under SQL connection limits.
    if not pg_pool:
        try:
            __connect(f'/cloudsql/{CONNECTION_NAME}')
        except OperationalError:
            # If production settings fail, use local development ones
            __connect('localhost')

    
    # Remember to close SQL resources declared while running this functions.
    # Keep any declared in global scope (e.g. pg_pool) for later reuse.
    with pg_pool.getconn() as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cursor:
            cursor.execute('INSERT INTO example_table VALUES (%s);', (request_json,))            
            return "OK"