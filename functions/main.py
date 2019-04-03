import uuid
import psycopg2
from psycopg2.extras import RealDictCursor

from util.cloudfunction import cloudfunction


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
            "dog_age": {"type": "integer", "minimum": 0, "maximum": 2147483647},
            "dog_breed": {"type": "string"},
            "dog_weight": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer", "minimum": 0},
                    "lower": {"type": "integer", "minimum": 0},
                    "upper": {"type": "integer", "minimum": 0}
                },
                "additionalProperties": False,
                "minProperties": 3,
            }
        },
        "additionalProperties": False,
        "minProperties": 4,
    })
def get_dog(request_json, conn):
    return _get_dog(request_json, conn)


def _get_dog(data, conn):
    id = data["id"]

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT image, age_months AS dog_age, breed AS dog_breed, weight_id as dog_weight
              FROM dogs
              WHERE id = %s""",
                       (id,))
        out = cursor.fetchone()
        cursor.execute("""
                        SELECT id, lower, upper 
                          FROM weights
                          WHERE id = %s""", (out["dog_weight"],))
        out["dog_weight"] = cursor.fetchone()

    # Convert from wacky in-memory format (postgres BYTEA) to byte-string
    out['image'] = str(bytes(out['image']), 'UTF-8')
    return out


@cloudfunction(
    in_schema={
        "type": "object",
        "properties": {
            "image": {"type": "string"},
            "dog_age": {"type": "integer", "minimum": 0},
            "dog_breed": {"type": "string"},
            "dog_weight": {"type": "integer", "minimum": 0},
            "user_email": {"type": "string"}
        },
        "additionalProperties": False,
        "minProperties": 5,
    },
    out_schema={
        "type": "integer"
    })
def submit_dog(request_json, conn):
    return _submit_dog(request_json, conn)


def _submit_dog(data, conn):
    """Submits the specified dog.
    :return: the id of the dog.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            'INSERT INTO dogs (image, age_months, breed, weight_id, submitter_email) VALUES (%s, %s, %s, %s, %s);',
            (data["image"],
             data["dog_age"],
             data["dog_breed"],
             data["dog_weight"],
             data["user_email"]))
        cursor.execute("SELECT id FROM dogs WHERE submitter_email = %s ORDER BY id DESC LIMIT 1",
                       (data["user_email"],))
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
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
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
        try:
            return cursor.fetchone()
        except psycopg2.ProgrammingError:
            return None


@cloudfunction(
    in_schema={
        "type": "object",
        "properties": {
            "gender_identity": {"oneOf": [{"type": "string"}, {"type": "null"}]},
            "age": {"oneOf": [{"type": "integer"}, {"type": "null"}]},
            "education": {"oneOf": [{"type": "integer", "minimum": 0, "maximum": 7}, {"type": "null"}]},
            "location": {"oneOf": [{"type": "string"}, {"type": "null"}]},
            "dog_ownership": {"oneOf": [{"type": "boolean"}, {"type": "null"}]},
            "northeastern_relationship": {"oneOf": [{"type": "string"}, {"type": "null"}]},
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
    return _register_voter(request_json, conn)


def _register_voter(data, conn):
    # TODO read this dynamically from the database to ensure SPoT
    education_levels = ['No high school',
                        'Some high school',
                        'High school diploma or equivalent',
                        'Vocational training',
                        'Some college',
                        'Associate\'s degree',
                        'Bachelor\'s degree',
                        'Post-undergraduate education']

    voter_uuid = str(uuid.uuid4())
    data["uuid"] = voter_uuid
    data["northeastern_affiliation"] = data["northeastern_relationship"]

    # To allow nulls, we only re-write the value if there is an integer there
    if data["education"] != None:
        data["education"] = education_levels[data["education"]]
    del data["northeastern_relationship"]

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
            data)

    return {**_get_dog_pair(voter_uuid, conn), "voter_uuid": voter_uuid}


@cloudfunction(
    in_schema={
        "type": "object",
        "properties": {
            "dog1_id": {"type": "integer", "minimum": 0},
            "dog2_id": {"type": "integer", "minimum": 0},
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
                "dog1": {"type": "integer", "minumum": 0},
                "dog2": {"type": "integer", "minumum": 0}
            },
            "additionalProperties": False,
            "minProperties": 2,
        }, {
            "type": "null"
        }]
    })
def submit_vote(request_json, conn):
    return _submit_vote(request_json, conn)


def _submit_vote(data, conn):
    id1 = data["dog1_id"]
    id2 = data["dog2_id"]
    winner = data["winner"]
    voter_uuid = data["voter_uuid"]

    # A mapping from `winner` to results in the vote.
    result = {
        -1: "tie",
        id1: "win",
        id2: "loss"
    }

    with conn.cursor() as cursor:
        cursor.execute("""SELECT id FROM voters WHERE uuid = %s""", (voter_uuid,))
        voter_id = cursor.fetchone()[0]
        cursor.execute("""INSERT INTO votes (dog1_id, dog2_id, result, voter_id) VALUES (%s, %s, %s, %s)""",
                       (id1, id2, result[winner], voter_id))

    return _get_dog_pair(voter_uuid, conn)


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
                    "wins": {"type": "integer", "minumum": 0},
                    "losses": {"type": "integer", "minumum": 0},
                    "ties": {"type": "integer", "minumum": 0}
                },
                "additionalProperties": False,
                "minProperties": 3,
            }
        },
        "additionalProperties": False,
    })
def get_votes(request_json, conn):
    return _get_votes(request_json, conn)


def _get_votes(request_json, conn):
    dog_id = request_json["id"]
    with conn.cursor() as cursor:
        select = """
        WITH wins AS (
          SELECT dog2_id AS dog FROM votes WHERE dog1_id = %(dog_id)s AND result = %(result1)s
          UNION ALL
          SELECT dog1_id AS dog FROM votes WHERE dog2_id = %(dog_id)s AND result = %(result2)s
        )
        SELECT dog, COUNT(*) FROM wins GROUP BY dog;
        """

        cursor.execute(select, {"dog_id": dog_id, "result1": "win", "result2": "loss"})
        wins = {str(row[0]): row[1] for row in cursor}

        cursor.execute(select, {"dog_id": dog_id, "result1": "loss", "result2": "win"})
        losses = {str(row[0]): row[1] for row in cursor}

        cursor.execute(select, {"dog_id": dog_id, "result1": "tie", "result2": "tie"})
        ties = {str(row[0]): row[1] for row in cursor}

        out = {}
        for key in {*wins.keys(), *losses.keys(), *ties.keys()}:
            out[key] = {
                "wins": wins.get(key, 0),
                "ties": ties.get(key, 0),
                "losses": losses.get(key, 0),
            }
        return out
