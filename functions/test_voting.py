import base64
import json
from unittest.mock import Mock

import pytest

from main import submit_dog, get_dog_pair, submit_vote, get_votes


def mock_request(data):
    return Mock(get_json=Mock(return_value=data), args=data)


def test_get_dog_pair():
    # if get_dog_pair(mock_request(None))[1] != 200:
    #     # If it fails, try adding two dogs, in hopes that the error was a lack of dogs in the db
    #     for i in range(2):
    #         image_data = b"Thisisatestimage"
    #         email = ''.join("test@example.com")
    #
    #         data = {
    #             "image": image_data,
    #             "dog_age": 12,
    #             "dog_breed": "mutt",
    #             "user_email": email,
    #             "dog_weight": 3
    #         }
    #
    #         request = mock_request(data)
    #         submit_dog(request)

    result = json.loads(get_dog_pair(mock_request(None))[0])
    id1 = result["dog1"]
    id2 = result["dog2"]
    assert id1 != id2

    voteFor1 = {
        "dog1_id": id1,
        "dog2_id": id2,
        "winner": id1
    }

    def get_votes_dog1(id1, id2):
        return json.loads(get_votes(mock_request({"id": id1}))[0]).get(str(id2), {"wins": 0, "losses": 0, "ties": 0})

    vote_count = get_votes_dog1(id1, id2)
    submit_vote(mock_request(voteFor1))

    vote_count_after = get_votes_dog1(id1, id2)

    assert vote_count_after["wins"] == vote_count["wins"] + 1
    assert vote_count_after["losses"] == vote_count["losses"]
    assert vote_count_after["ties"] == vote_count["ties"]

    result = json.loads(get_dog_pair(mock_request(None))[0])
    id1 = result["dog1"]
    id2 = result["dog2"]
    assert id1 != id2

    tie_vote = {
        "dog1_id": id1,
        "dog2_id": id2,
        "winner": -1
    }

    vote_count = get_votes_dog1(id1, id2)
    submit_vote(mock_request(tie_vote))
    vote_count_after = get_votes_dog1(id1, id2)
    assert vote_count_after["wins"] == vote_count["wins"]
    assert vote_count_after["losses"] == vote_count["losses"]
    assert vote_count_after["ties"] == vote_count["ties"] + 1
