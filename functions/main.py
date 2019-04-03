import uuid

from util.cloudfunction import cloudfunction
from jsonschema import validate


@cloudfunction(
    in_schema={
        "type": "object",
        "properties": {
            "id": {"type": "integer"}
        },
        "additionalProperties": False,
        "minProperties": 1,
    },
    out_schema={
        "type": "object",
        "properties": {
            "image": {"type": "string"},
            "dog_age": {"type": "number"},
            "dog_breed": {"type": "string"},
            "dog_weight": {
                "type": "object",
                "properties": {
                    "id": {"type": "number"},
                    "lower": {"type": "number"},
                    "upper": {"type": "number"}
                },
                "additionalProperties": False,
                "minProperties": 3,
            }
        },
        "additionalProperties": False,
        "minProperties": 4,
    })
def get_dog(request_json, conn):
    id = request_json["id"]

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


@cloudfunction(
    in_schema={
        "type": "object",
        "properties": {
            "user_email": {"type": "string"}
        },
        "additionalProperties": False,
        "minProperties": 1,
    },
    out_schema={
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "image": {"type": "string"},
                "dog_age": {"type": "number"},
                "dog_breed": {"type": "string"},
                "submission_time": {"type": "string"},
                "dog_weight": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "number"},
                        "lower": {"type": "number"},
                        "upper": {"type": "number"}
                    },
                    "additionalProperties": False,
                    "minProperties": 3,
                }
            }
        },
        "additionalProperties": False,
        "minProperties": 5,
    })
def get_submissions(request_json, conn):
    submitter_email = request_json["user_email"]

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


@cloudfunction(
    in_schema={
        "type": "object",
        "properties": {
            "image": {"type": "string"},
            "dog_age": {"type": "integer"},
            "dog_breed": {"type": "string"},
            "dog_weight": {"type": "integer"},
            "user_email": {"type": "string"}
        },
        "additionalProperties": False,
        "minProperties": 5,
    },
    out_schema={
        "type": "number"
    })
def submit_dog(request_json, conn):
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
    return id


@cloudfunction(
    in_schema={"type": "string"},
    out_schema={
        "anyOf": [{
            "type": "object",
            "properties": {
                "dog1": {"type": "integer"},
                "dog2": {"type": "integer"}
            },
            "additionalProperties": False,
            "minProperties": 2,
        }, {
            "type": "null"
        }]
    })
def get_dog_pair(request_json, conn):
    return _get_dog_pair(request_json, conn)


def _get_dog_pair(voter_uuid, conn):
    with conn.cursor() as cursor:
        cursor.execute("""
        WITH dogs1 AS (SELECT dogs.id AS dog1 FROM dogs),
             dogs2 AS (SELECT dogs.id AS dog2 FROM dogs),
             dog_pairs AS (SELECT * FROM dogs1 CROSS JOIN dogs2 WHERE dog1 != dog2),
             seen_pairs AS (
              SELECT dog1_id, dog2_id AS dog FROM votes JOIN voters ON (voter_id = voters.id) WHERE voters.uuid = %(vid)s
              UNION ALL 
              SELECT dog2_id, dog1_id AS dog FROM votes JOIN voters ON (voter_id = voters.id) WHERE voters.uuid = %(vid)s
             )
        
        SELECT dog1, dog2 FROM dog_pairs
        WHERE (dog1, dog2) NOT IN (SELECT * FROM seen_pairs)
          AND (dog2, dog1) NOT IN (SELECT * FROM seen_pairs)
        ORDER BY RANDOM()
        LIMIT 1
        """, {"vid": voter_uuid})
        ids = cursor.fetchone()
    return {
        "dog1": ids[0],
        "dog2": ids[1]
    }


@cloudfunction(
    in_schema={
        "type": "object",
        "properties": {
            "dog1_id": {"type": "integer"},
            "dog2_id": {"type": "integer"},
            "winner": {"type": "integer"},
            "voter_uuid": {"type": "string"},
        },
        "additionalProperties": False,
        "minProperties": 4,
    },
    out_schema={
        "anyOf": [{
            "type": "object",
            "properties": {
                "dog1": {"type": "integer"},
                "dog2": {"type": "integer"}
            },
            "additionalProperties": False,
            "minProperties": 2,
        }, {
            "type": "null"
        }]
    })
def submit_vote(request_json, conn):
    id1 = request_json["dog1_id"]
    id2 = request_json["dog2_id"]
    winner = request_json["winner"]
    voter_uuid = request_json["voter_uuid"]

    # A mapping from `winner` to results in the vote.
    result = {
        -1: "tie",
        id1: "win",
        id2: "loss"
    }

    with conn.cursor() as cursor:
        cursor.execute('''SELECT id FROM voters WHERE uuid = %s''', (voter_uuid,))
        numeric_voter_id = cursor.fetchone()[0]
        cursor.execute("""INSERT INTO votes (dog1_id, dog2_id, result, voter_id) VALUES (%s, %s, %s, %s)""",
                       (id1, id2, result[winner], numeric_voter_id))

    return _get_dog_pair(voter_uuid, conn)


@cloudfunction(
    in_schema={
        "type": "object",
        "properties": {
            "gender_identity": {"type": ["string", "null"]},
            "age": {"type": ["integer", "null"]},
            "education": {"type": ["integer", "null"]},
            "location": {"type": ["string", "null"]},
            "dog_ownership": {"type": ["boolean", "null"]},
            "northeastern_relationship": {"type": ["string", "null"]},
        },
        "additionalProperties": False,
        "minProperties": 6,
    },
    out_schema={
        "anyOf": [{
            "type": "object",
            "properties": {
                "dog1": {"type": "integer"},
                "dog2": {"type": "integer"},
                "voter_uuid": {"type": "string"},
            },
            "additionalProperties": False,
            "minProperties": 2,
        }, {
            "type": "null"
        }]
    })
def register_voter(request_json, conn):
    education_levels = ['No high school',
                        'Some high school',
                        'High school diploma or equivalent',
                        'Vocational training',
                        'Some college',
                        'Associate\'s degree',
                        'Bachelor\'s degree',
                        'Post-undergraduate education']

    voter_uuid = str(uuid.uuid4())
    request_json["uuid"] = voter_uuid
    request_json["northeastern_affiliation"] = request_json["northeastern_relationship"]
    if request_json["education"] != None:
        request_json["education"] = education_levels[request_json["education"]]
    del request_json["northeastern_relationship"]

    with conn.cursor() as cursor:
        cursor.execute(
            """INSERT INTO voters (uuid, gender_identity, age, 
            education, location, dog_ownership, northeastern_affiliation) 
            VALUES (%(uuid)s, 
            %(gender_identity)s, 
            %(age)s, 
            %(education)s, 
            %(location)s, 
            %(dog_ownership)s, 
            %(northeastern_affiliation)s
            )""",
            request_json)

    return {**_get_dog_pair(voter_uuid, conn), "voter_uuid": voter_uuid}


@cloudfunction(
    in_schema={
        "type": "object",
        "properties": {
            "id": {"type": "integer"}
        },
        "additionalProperties": False,
        "minProperties": 1,
    },
    out_schema={
        "type": "object",
        "patternProperties": {
            # this one is hard to read, but says keys are strings made of digits (dog ids)
            "^[0-9]+$": {
                "type": "object",
                "properties": {
                    "wins": {"type": "number"},
                    "losses": {"type": "number"},
                    "ties": {"type": "number"}
                },
                "additionalProperties": False,
                "minProperties": 3,
            }
        },
        "additionalProperties": False,
    })
def get_votes(request_json, conn):
    dog_id = request_json["id"]
    with conn.cursor() as cursor:
        select = """
        WITH wins AS (
          SELECT dog2_id AS dog FROM votes WHERE dog1_id = %s AND result = %s
          UNION ALL
          SELECT dog1_id AS dog FROM votes WHERE dog2_id = %s AND result = %s
        )
        SELECT dog, COUNT(*) FROM wins GROUP BY dog;
        """

        cursor.execute(select, (dog_id, 'win', dog_id, 'loss'))
        wins = {str(row[0]): row[1] for row in cursor}

        cursor.execute(select, (dog_id, 'loss', dog_id, 'win'))
        losses = {str(row[0]): row[1] for row in cursor}

        cursor.execute(select, (dog_id, 'tie', dog_id, 'tie'))
        ties = {str(row[0]): row[1] for row in cursor}

        out = {}
        for key in {*wins.keys(), *losses.keys(), *ties.keys()}:
            out[key] = {
                "wins": wins.get(key, 0),
                "ties": ties.get(key, 0),
                "losses": losses.get(key, 0),
            }
        return out
