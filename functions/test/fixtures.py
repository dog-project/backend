import random

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
    c = connect_to_clean_db()
    yield c
    c.close()


@pytest.fixture()
def populated_database_conn():
    conn = connect_to_clean_db()

    with conn.cursor() as cursor:
        for _ in range(10):
            cursor.execute("""INSERT INTO dogs (image, age_months, weight_id, breed, submitter_email) 
                                VALUES (%s, %s, %s, %s, %s)""",
                           ("image", 0, 0, "test", "test@example.com"))
        uuid = "c5bca70d-4d32-4ae5-bacb-93774bb17e3f"
        cursor.execute("""INSERT INTO voters (uuid, gender_identity, age, education, location, dog_ownership, northeastern_affiliation) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)""", (uuid, None, None, None, None, None, None))
        for _ in range(1000):
            dog1 = random.randint(1, 10)
            dog2 = random.randint(1, 10)
            winner = ["win", "loss", "tie"][random.randint(0, 2)]
            cursor.execute("""INSERT INTO votes (voter_id, dog1_id, dog2_id, result) VALUES (%s, %s, %s, %s) """,
                           (1, dog1, dog2, winner))

    yield conn
    conn.close()

