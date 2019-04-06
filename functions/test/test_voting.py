import pytest

from main import _register_voter, _submit_vote, _get_votes
from test.fixtures import populated_database_conn

def test_voting_changes_vote_counts(populated_database_conn):
    conn = populated_database_conn
    voter = {
        "gender_identity": None,
        "age": None,
        "education": None,
        "location": None,
        "dog_ownership": None,
        "northeastern_relationship": None,
    }

    register_result = _register_voter(voter, conn)
    uuid = register_result["voter_uuid"]

    # Voting for dog1
    dog1 = register_result["dog1"]
    dog2 = register_result["dog2"]
    votes1_before = _get_votes({"id": dog1}, conn)
    votes2_before = _get_votes({"id": dog2}, conn)

    result = _submit_vote({"voter_uuid": uuid, "dog1_id": dog1, "dog2_id": dog2, "winner": dog1}, conn)

    votes1_after = _get_votes({"id": dog1}, conn)
    votes2_after = _get_votes({"id": dog2}, conn)

    # dog1 has one additional win against dog2
    # dog2 has one additional loss against dog1
    assert votes1_before.get(str(dog2), {}).get("wins", 0) + 1 == votes1_after[str(dog2)]["wins"]
    assert votes2_before.get(str(dog1), {}).get("losses", 0) + 1 == votes2_after[str(dog1)]["losses"]

    # Voting for dog2
    dog1 = result["dog1"]
    dog2 = result["dog2"]
    votes1_before = _get_votes({"id": dog1}, conn)
    votes2_before = _get_votes({"id": dog2}, conn)

    result = _submit_vote({"voter_uuid": uuid, "dog1_id": dog1, "dog2_id": dog2, "winner": dog2}, conn)

    votes1_after = _get_votes({"id": dog1}, conn)
    votes2_after = _get_votes({"id": dog2}, conn)

    # dog1 has one additional win against dog2
    # dog2 has one additional loss against dog1
    assert votes1_before.get(str(dog2), {}).get("losses", 0) + 1 == votes1_after[str(dog2)]["losses"]
    assert votes2_before.get(str(dog1), {}).get("wins", 0) + 1 == votes2_after[str(dog1)]["wins"]

    # Tie
    dog1 = result["dog1"]
    dog2 = result["dog2"]
    votes1_before = _get_votes({"id": dog1}, conn)
    votes2_before = _get_votes({"id": dog2}, conn)

    result = _submit_vote({"voter_uuid": uuid, "dog1_id": dog1, "dog2_id": dog2, "winner": -1}, conn)

    votes1_after = _get_votes({"id": dog1}, conn)
    votes2_after = _get_votes({"id": dog2}, conn)

    # dog1 has one additional win against dog2
    # dog2 has one additional loss against dog1
    assert votes1_before.get(str(dog2), {}).get("losses", 0) == votes1_after[str(dog2)]["losses"]
    assert votes1_before.get(str(dog2), {}).get("wins", 0) == votes1_after[str(dog2)]["wins"]
    assert votes1_before.get(str(dog2), {}).get("ties", 0) + 1 == votes1_after[str(dog2)]["ties"]

    assert votes2_before.get(str(dog1), {}).get("losses", 0) == votes2_after[str(dog1)]["losses"]
    assert votes2_before.get(str(dog1), {}).get("wins", 0) == votes2_after[str(dog1)]["wins"]
    assert votes2_before.get(str(dog1), {}).get("ties", 0) + 1 == votes2_after[str(dog1)]["ties"]


@pytest.mark.slow
def test_vote_duplicate(conn):
    voter = {
        "gender_identity": "masculine",
        "age": 20,
        "education": 0,
        "location": "china",
        "dog_ownership": False,
        "northeastern_relationship": "full-time student",
    }

    seen_pairs = set()
    num_votes = 0

    register_result = _register_voter(voter, conn)
    uuid = register_result["voter_uuid"]
    dog1 = register_result["dog1"]
    dog2 = register_result["dog2"]

    while num_votes < 1000:
        seen_pairs.add((dog1, dog2))
        num_votes += 1

        result = _submit_vote({"voter_uuid": uuid, "dog1_id": dog1, "dog2_id": dog2, "winner": dog1}, conn)
        if result is None:
            break
        dog1 = result["dog1"]
        dog2 = result["dog2"]

    assert num_votes == len(seen_pairs)
