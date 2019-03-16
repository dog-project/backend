import base64
import datetime
import random
import string
from unittest.mock import Mock

from functions.submit_dog.main import submit_dog
from get_dog.main import get_dog
from get_submission.main import get_submission


def mock_request(data):
    return Mock(get_json=Mock(return_value=data), args=data)

def test_submit_dog_happy_path():
    image_data = base64.b64encode(b"This is a test image")
    email = ''.join(random.choices(string.ascii_lowercase, k=30)) + "@example.com"

    data = {
        "image": image_data,
        "age_months": 12,
        "breed": "mutt",
        "submitter_email": email,
    }

    request = mock_request(data)
    r = submit_dog(request)
    assert r['status'] == "OK"
    assert 'id' in r
    dog_id = r['id']

    dog_data = get_dog(mock_request({"id": dog_id}))
    assert dog_data["image"] == image_data
    assert dog_data["age_months"] == 12
    assert dog_data["breed"] == "mutt"


    submission_data = get_submission(mock_request({"submitter_email": email}))
    assert submission_data["image"] == image_data
    assert submission_data["age_months"] == 12
    assert submission_data["breed"] == "mutt"
    # Reasonable performance req
    assert (datetime.datetime.now() - submission_data['submission_time']).total_seconds() < 10