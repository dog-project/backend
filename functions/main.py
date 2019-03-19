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
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("""
            SELECT image, age_months, breed
              FROM dogs
              WHERE id = %s""",
                           (id,))
            out = cursor.fetchone()
    # Convert from wacky in-memory format to byte-string, BYTEA
    out['image'] = bytes(out['image'])
    return out


def get_submission(request):
    global pg_pool

    submitter_email = request.get_json()["submitter_email"]
    assert isinstance(submitter_email, str)

    if not pg_pool:
        pg_pool = get_pool()

    with pg_pool.getconn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("""
            SELECT image, age_months, breed, submission_time
              FROM dogs
              WHERE submitter_email = %s""",
                           (submitter_email,))
            out = cursor.fetchone()
    # Convert from wacky in-memory format to byte-string, BYTEA
    out['image'] = bytes(out['image'])
    return out


def submit_dog(request):
    global pg_pool

    request_json = request.get_json()

    if not pg_pool:
        pg_pool = get_pool()

    with pg_pool.getconn() as conn:
        with conn.cursor() as cursor:
            cursor.execute('INSERT INTO dogs (image, age_months, breed, submitter_email) VALUES (%s, %s, %s, %s);',
                           (request_json["image"],
                            request_json["age_months"],
                            request_json["breed"],
                            request_json["submitter_email"]))

            cursor.execute("SELECT id FROM dogs WHERE submitter_email = %s", (request_json["submitter_email"],))
            id = cursor.fetchone()[0]

        conn.commit()
    return {"status": "OK", "id": id}
