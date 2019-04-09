from main import _submit_vote, _get_ranking, _list_dogs, _register_voter
from test.fixtures import populated_database_conn

def test_get_ranking(populated_database_conn):
    conn = populated_database_conn

    ids = _list_dogs(conn)

    voter_data = {
            "gender_identity": "test",
            "age": 20,
            "education": 0,
            "location": "test",
            "dog_ownership": False,
            "northeastern_relationship": "test",
        }

    registration_result = _register_voter(voter_data, conn)
    uuid = registration_result["voter_uuid"]

    id1 = ids[0]
    id2 = ids[1]

    _submit_vote({
        "dog1_id": id1,
        "dog2_id": id2,
        "winner": id1,
        "voter_uuid": uuid,
    }, conn)

    ranking = _get_ranking(conn)
    assert ranking[0] == id1

    # TODO this test is insufficient, test with more than one vote
