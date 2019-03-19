import json

import psycopg2.extras

from util.get_pool import get_pool

pg_pool = None

def get_dog(request):
    global pg_pool

    id = request.get_json()["id"]
    assert isinstance(id, int)

    if not pg_pool:
        pg_pool = get_pool()

    with pg_pool.getconn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT image, age_months, breed, weight_id
              FROM dogs
              WHERE id = %s""",
                           (id,))
            out = dict(zip(
                ("image", "dog_age", "dog_breed", "dog_weight"),
                cursor.fetchone()))
            cursor.execute("""
                        SELECT id, lower, upper 
                          FROM weights
                          WHERE id = %s""", (out["dog_weight"],))
            out["dog_weight"] = dict(zip(("id", "lower", "upper"), cursor.fetchone()))
    # Convert from wacky in-memory format to byte-string, BYTEA
    out['image'] = str(bytes(out['image']), 'UTF-8')
    return json.dumps(out)


def get_submission(request):
    global pg_pool

    submitter_email = request.get_json()["user_email"]
    assert isinstance(submitter_email, str)

    if not pg_pool:
        pg_pool = get_pool()

    with pg_pool.getconn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT image, age_months, breed, weight_id, submission_time
              FROM dogs
              WHERE submitter_email = %s""",
                           (submitter_email,))
            out = dict(zip(
                ("image", "dog_age", "dog_breed", "dog_weight", "submission_time"),
                cursor.fetchone()))
            cursor.execute("""
                        SELECT id, lower, upper 
                          FROM weights
                          WHERE id = %s""", (out["dog_weight"],))
            out["dog_weight"] = dict(zip(("id", "lower", "upper"), cursor.fetchone()))
    # Convert from wacky in-memory format to byte-string, BYTEA
    out['image'] = str(bytes(out['image']), 'UTF-8')
    out['submission_time'] = str(out['submission_time'])
    return json.dumps(out)


def submit_dog(request):
    global pg_pool

    request_json = request.get_json()

    if not pg_pool:
        pg_pool = get_pool()

    with pg_pool.getconn() as conn:
        with conn.cursor() as cursor:
            cursor.execute('INSERT INTO dogs (image, age_months, breed, weight_id, submitter_email) VALUES (%s, %s, %s, %s, %s);',
                           (request_json["image"],
                            request_json["dog_age"],
                            request_json["dog_breed"],
                            request_json["dog_weight"],
                            request_json["user_email"]))
            cursor.execute("SELECT id FROM dogs WHERE submitter_email = %s", (request_json["user_email"],))
            id = cursor.fetchone()[0]


        conn.commit()
    return json.dumps({"status": "OK", "id": id})
