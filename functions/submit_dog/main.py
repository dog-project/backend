from util.get_pool import get_pool

pg_pool = None

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
