from hypothesis import given, HealthCheck, settings, example, strategies as st
from hypothesis_jsonschema import from_schema

from main import _register_voter, _submit_vote, _get_votes

from util.get_pool import get_connection


@settings(suppress_health_check=HealthCheck.all())
@example({
    "gender_identity": "masculine",
    "age": 20,
    "education": 0,
    "location": "china",
    "dog_ownership": False,
    "northeastern_relationship": "full-time student",
})
@given(from_schema({
    "type": "object",
    "properties": {
        "gender_identity": {"oneOf": [{"type": "string"}, {"type": "null"}]},
        "age": {"oneOf": [{"type": "integer", "minimum": 0, "maximum": 2147483647}, {"type": "null"}]},
        "education": {"oneOf": [{"type": "integer", "minimum": 0, "maximum": 7}, {"type": "null"}]},
        "location": {"oneOf": [{"type": "string"}, {"type": "null"}]},
        "dog_ownership": {"oneOf": [{"type": "boolean"}, {"type": "null"}]},
        "northeastern_relationship": {"oneOf": [{"type": "string"}, {"type": "null"}]},
    },
    "required": [
        "gender_identity",
        "age",
        "education",
        "location",
        "dog_ownership",
        "northeastern_relationship",
    ]
}))
def test_register_voting_schema(s):
    with get_connection() as conn:
        _register_voter(s, conn)


@settings(suppress_health_check=HealthCheck.all())
@given(from_schema({
    "type": "object",
    "properties": {
        "dog1_id": {"type": "integer", "minimum": 0},
        "dog2_id": {"type": "integer", "minimum": 0},
        "winner": {"type": "integer"},
        "voter_uuid": {"type": "string"},
    },
    "required": [
        "gender_identity",
        "age",
        "education",
        "location",
        "dog_ownership",
        "northeastern_relationship",
    ]
}))
def test_voting(s):
    voter = {
        "gender_identity": "masculine",
        "age": 20,
        "education": 0,
        "location": "china",
        "dog_ownership": False,
        "northeastern_relationship": "full-time student",
    }

    with get_connection() as conn:
        register_result = _register_voter(voter, conn)
        uuid = register_result["voter_uuid"]
        dog1 = register_result["dog1"]
        dog2 = register_result["dog2"]

        votes1_before = _get_votes({"id": dog1}, conn)
        votes2_before = _get_votes({"id": dog2}, conn)

        _submit_vote({"voter_uuid": uuid, "dog1_id": dog1, "dog2_id": dog2, "winner": dog1}, conn)

        votes1_after = _get_votes({"id": dog1}, conn)
        votes2_after = _get_votes({"id": dog2}, conn)

        print(votes1_before, votes1_after)

        assert votes1_before.get(str(dog2), {}).get("wins", 0) + 1 == votes1_after[str(dog2)]["wins"]
        del votes1_before[dog2]
        del votes1_after[dog2]
        assert votes1_before == votes2_after
