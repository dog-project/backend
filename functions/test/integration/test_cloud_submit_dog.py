import base64
import json
from unittest.mock import Mock

from main import submit_dog
from main import get_dog

from util.get_pool import get_pool


def mock_request(data):
    return Mock(get_json=Mock(return_value=data), args=data)

def test_submit_dog_happy_path():
    pool = get_pool()
    with pool.getconn() as conn:
        image_data = str(base64.b64encode(b"This is a test image"), 'utf8')
        email = ''.join("test@example.com")

        data = {
            "image": image_data,
            "dog_age": 12,
            "dog_breed": "mutt",
            "user_email": email,
            "dog_weight": 0
        }

        r = json.loads(submit_dog(mock_request(data))[0])
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

        pool.putconn(conn)
