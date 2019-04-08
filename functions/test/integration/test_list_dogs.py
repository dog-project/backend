from main import _list_dogs, _submit_dog, _get_dog, _get_votes
from test.fixtures import conn, populated_database_conn


def test_add_dog_only_submitted_dog_id(conn):
    dog_data = {
            "image": "base64",
            "dog_age": 1,
            "dog_breed": "test",
            "dog_weight": 0,
            "user_email": "test"
    }
    dog_id = _submit_dog(dog_data, conn)

    dogs_after = _list_dogs(conn)
    assert dogs_after == [dog_id]

def test_list_dogs_contains_submitted_dog_id(populated_database_conn):
    conn = populated_database_conn

    dogs_before = _list_dogs(conn)

    dog_data = {
            "image": "base64",
            "dog_age": 1,
            "dog_breed": "test",
            "dog_weight": 0,
            "user_email": "test"
    }
    dog_id = _submit_dog(dog_data, conn)

    dogs_after = _list_dogs(conn)
    assert dogs_after == dogs_before + [dog_id]

def test_list_dogs_all_dogs_valid_for_get_votes(conn):
    for dog in _list_dogs(conn):
        _get_votes({"id": dog}, conn)
        _get_dog({"id": dog}, conn)
