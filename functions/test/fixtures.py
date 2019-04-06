import pytest

from util.get_pool import get_connection

def connect_to_clean_db():
    conn = get_connection()

    with open("sql/teardown.sql") as teardown:
        with conn.cursor() as cursor:
            cursor.execute("\n".join(teardown))

    with open("sql/setup.sql") as setup:
        with conn.cursor() as cursor:
            cursor.execute("\n".join(setup))

    return conn


@pytest.fixture()
def conn():
    return connect_to_clean_db()


@pytest.fixture()
def populated_database_conn():
    conn = connect_to_clean_db()

    with conn.cursor() as cursor:
        for _ in range(10):
            cursor.execute("""INSERT INTO dogs (image, age_months, weight_id, breed, submitter_email) 
                                VALUES (%s, %s, %s, %s, %s)""",
                           ("image", 0, 0, "test", "test@example.com"))
    return conn

