import base64
import datetime

from main import _submit_dog
from main import _get_dog

from util.get_pool import get_pool


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

        r = _submit_dog(data, conn)
        dog_id = r

        dog_data = _get_dog({"id": dog_id}, conn)
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
