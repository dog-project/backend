from util.cloudfunction import cloudfunction


@cloudfunction
def get_dog(request_json, conn):
    id = request_json["id"]
    assert isinstance(id, int)

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
    return out


@cloudfunction
def get_submissions(request_json, conn):
    submitter_email = request_json["user_email"]
    assert isinstance(submitter_email, str)

    with conn.cursor() as cursor:
        cursor.execute("""
        SELECT image, age_months, breed, weight_id, submission_time
          FROM dogs
          WHERE submitter_email = %s
          ORDER BY id;""",
                       (submitter_email,))
        out = []
        for row in cursor:
            row_dict = dict(zip(
                ("image", "dog_age", "dog_breed", "dog_weight", "submission_time"),
                row))

            with conn.cursor() as weight_cursor:
                weight_cursor.execute("""
                        SELECT id, lower, upper 
                          FROM weights
                          WHERE id = %s;""", (row_dict["dog_weight"],))
                row_dict["dog_weight"] = dict(zip(("id", "lower", "upper"), weight_cursor.fetchone()))

            # Convert from wacky in-memory format to byte-string, BYTEA
            row_dict['image'] = str(bytes(row_dict['image']), 'UTF-8')
            row_dict['submission_time'] = str(row_dict['submission_time'])
            out.append(row_dict)

    return out


@cloudfunction
def submit_dog(request_json, conn):
    request_json = request_json

    with conn.cursor() as cursor:
        cursor.execute(
            'INSERT INTO dogs (image, age_months, breed, weight_id, submitter_email) VALUES (%s, %s, %s, %s, %s);',
            (request_json["image"],
             request_json["dog_age"],
             request_json["dog_breed"],
             request_json["dog_weight"],
             request_json["user_email"]))
        cursor.execute("SELECT id FROM dogs WHERE submitter_email = %s ORDER BY id DESC LIMIT 1",
                       (request_json["user_email"],))
        id = cursor.fetchone()[0]

    conn.commit()
    out = {"status": "OK", "id": id}
    return out
