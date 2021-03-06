import json
from unittest.mock import Mock

from main import get_dog_pair, submit_vote, get_votes, register_voter


def mock_request(data):
    return Mock(get_json=Mock(return_value=data), args=data)


def test_vote():
    voter_data = {
            "gender_identity": "test",
            "age": 20,
            "education": 0,
            "location": "test",
            "dog_ownership": False,
            "northeastern_relationship": "test",
        }

    result = json.loads(register_voter(mock_request(voter_data))[0])
    id1 = result["dog1"]
    id2 = result["dog2"]
    voter_uuid = result["voter_uuid"]
    assert id1 != id2

    voteFor1 = {
        "dog1_id": id1,
        "dog2_id": id2,
        "winner": id1,
        "voter_uuid": voter_uuid,
    }

    def get_votes_dog1(id1, id2):
        return json.loads(get_votes(mock_request({"id": id1}))[0]).get(str(id2), {"wins": 0, "losses": 0, "ties": 0})

    vote_count = get_votes_dog1(id1, id2)
    submit_vote(mock_request(voteFor1))

    vote_count_after = get_votes_dog1(id1, id2)

    assert vote_count_after["wins"] == vote_count["wins"] + 1
    assert vote_count_after["losses"] == vote_count["losses"]
    assert vote_count_after["ties"] == vote_count["ties"]

    result = json.loads(get_dog_pair(mock_request(voter_uuid))[0])
    id1 = result["dog1"]
    id2 = result["dog2"]
    assert id1 != id2

    tie_vote = {
        "dog1_id": id1,
        "dog2_id": id2,
        "winner": -1,
        "voter_uuid": voter_uuid,
    }

    vote_count = get_votes_dog1(id1, id2)
    submit_vote(mock_request(tie_vote))
    vote_count_after = get_votes_dog1(id1, id2)
    assert vote_count_after["wins"] == vote_count["wins"]
    assert vote_count_after["losses"] == vote_count["losses"]
    assert vote_count_after["ties"] == vote_count["ties"] + 1


