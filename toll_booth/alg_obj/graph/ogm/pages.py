import json

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.aws.aws_obj.squirrel import SneakyKipper


class PaginationToken(AlgObject):
    def __init__(self, username, inclusive_start=0, exclusive_end=10, pagination_id=None):
        if not pagination_id:
            import uuid
            pagination_id = uuid.uuid4().hex
        self._username = username
        self._inclusive_start = inclusive_start
        self._exclusive_end = exclusive_end
        self._pagination_id = pagination_id

    @classmethod
    def parse_json(cls, json_dict):
        from toll_booth.alg_obj.aws.aws_obj.squirrel import SneakyKipper
        token = json_dict.get('token', None)
        username = json_dict.get('username')
        if not token:
            return cls(username, exclusive_end=json_dict.get('page_size', 10))
        json_string = SneakyKipper('pagination').decrypt(token, {'username': username})
        obj_dict = json.loads(json_string)
        return cls(username, pagination_id=obj_dict['id'],
                   inclusive_start=obj_dict['start'], exclusive_end=obj_dict['end'])

    @property
    def to_gql(self):
        encrypted_value = self.package()
        return encrypted_value

    @property
    def inclusive_start(self):
        return self._inclusive_start

    @property
    def exclusive_end(self):
        return self._exclusive_end

    def increment(self):
        step_value = self._exclusive_end - self._inclusive_start
        self._inclusive_start += step_value
        self._exclusive_end += step_value

    def package(self):
        from toll_booth.alg_obj.aws.aws_obj.squirrel import SneakyKipper
        unencrypted_text = json.dumps({
            'id': self._pagination_id,
            'start': self._inclusive_start,
            'end': self._exclusive_end
        })
        return SneakyKipper('pagination').encrypt(unencrypted_text, {'username': self._username})
