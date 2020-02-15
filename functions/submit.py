import psycopg2
import uuid

from util.cloudfunction import cloudfunction

@cloudfunction(
    in_schema={
        # Every input to the database is expected as the type in the database
        # This code assumes that tiers are arrays in the database
        "type": "object",
        "properties": {
            "state": {"type": "string"},
            "age_above_18": {"type": "boolean"},
            "eligible": {"type": "boolean"},
            "race": {"type": "array", "items": {"type": "string"}},
            "gender": {"oneOf": [{"type": "string"}, {"type": "null"}]},
            "education": {"oneOf": [{"type": "string"}, {"type": "null"}]},
            "age": {"oneOf": [{"type": "string"}, {"type": "null"}]},
            "party": {"oneOf": [{"type": "string"}, {"type": "null"}]},
            "lgbtq": {"oneOf": [{"type": "boolean"}, {"type": "null"}]},
            "top_candidate": {"type": "string"},
            "tier1": {"type": "array", "items": {"type":  "string"}},
            "tier2": {"type": "array", "items": {"type":  "string"}},
            "tier3": {"type": "array", "items": {"type":  "string"}},
            "tier4": {"type": "array", "items": {"type":  "string"}},
            "tier5": {"type": "array", "items": {"type":  "string"}},
            "tier6": {"type": "array", "items": {"type":  "string"}}
        },
        "additionalProperties": False,
        "minProperties": 10
    },
    out_schema={"type": "null"}
    )

def submit(request_json, conn):
    return _submit(request_json, conn)

def _submit(data, conn):
    voter_id = uuid.uuid4().int

    cursor = conn.cursor()

    cursor.execute("""INSERT INTO voters
    (id, state, age_above_18, eligible, race, gender, education, age, party, lgbtq) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
    (voter_id, data["state"], data["age_above_18"], data["eligible"], data["race"], data["gender"], data["education"], data["age"], data["party"], data["lgbtq"]))

    cursor.execute("""INSERT INTO ballot 
    (voter, top_candidate, tier1, tier2, tier3, tier4, tier5, tier6) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
    (voter_id, data["top_candidate"], data["tier1"], data["tier2"], data["tier3"], data["tier4"], data["tier5"], data["tier6"]))

    return None