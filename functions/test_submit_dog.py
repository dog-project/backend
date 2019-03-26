import base64
import datetime
import json
import random
import string
from unittest.mock import Mock

from main import submit_dog
from main import get_dog
from main import get_submissions


def mock_request(data):
    return Mock(get_json=Mock(return_value=data), args=data)

def test_submit_dog_happy_path():
    image_data = str(base64.b64encode(b"This is a test image"), 'utf8')
    email = ''.join("test@example.com")

    data = {
        "image": image_data,
        "dog_age": 12,
        "dog_breed": "mutt",
        "user_email": email,
        "dog_weight": 0
    }

    request = mock_request(data)
    r = json.loads(submit_dog(request)[0])
    dog_id = r

    dog_data = json.loads(get_dog(mock_request({"id": dog_id}))[0])
    assert dog_data["image"] == image_data, 'UTF-8'
    assert dog_data["dog_age"] == 12
    assert dog_data["dog_breed"] == "mutt"
    assert dog_data["dog_weight"] == \
           {
           "id": 0,
           "lower": 0,
           "upper": 12  # Assumes existence of a range 0 to 12 at index 0
           }


    submissions = json.loads(get_submissions(mock_request({"user_email": email}))[0])
    print(submissions)
    submission_data = submissions[-1]

    assert submission_data["image"] == image_data, 'UTF-8'
    assert submission_data["dog_age"] == 12
    assert submission_data["dog_breed"] == "mutt"
    assert submission_data["dog_weight"] == {'id': 0, 'lower': 0, 'upper': 12}
    # Reasonable performance req
    assert (datetime.datetime.now()
            - datetime.datetime.strptime(submission_data['submission_time'], "%Y-%m-%d %H:%M:%S.%f"))\
               .total_seconds()\
           < 10
