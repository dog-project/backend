import psycopg2.extras

from util.get_pool import get_pool

pg_pool = None

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
