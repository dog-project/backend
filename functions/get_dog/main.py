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
