import json

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.aws.squirrels.squirrel import SneakyKipper


class PaginationToken(AlgObject):
    def __init__(self, username, source, context, inclusive_start=0, exclusive_end=10, pagination_id=None):
        if not pagination_id:
            import uuid
            pagination_id = uuid.uuid4().hex
        self._username = username
        self._source = source
        self._context = context
        self._inclusive_start = inclusive_start
        self._exclusive_end = exclusive_end
        self._pagination_id = pagination_id

    @classmethod
    def parse_json(cls, json_dict):
        try:
            return cls(
                json_dict['username'], json_dict['source'], json_dict['context'],
                json_dict['inclusive_start'], json_dict['exclusive_end'], json_dict['pagination_id']
            )
        except KeyError:
            token = json_dict['token']
            username = json_dict['username']
            source = json_dict['source']
            context = json_dict['context']
            if not token:
                return cls(username, source, context, exclusive_end=json_dict['page_size'])
            json_string = SneakyKipper('pagination').decrypt(token, {'username': username})
            obj_dict = json.loads(json_string)
            return cls(username, obj_dict['source'], obj_dict['context'], pagination_id=obj_dict['id'],
                       inclusive_start=obj_dict['start'], exclusive_end=obj_dict['end'])

    @property
    def to_gql(self):
        encrypted_value = self.package()
        return encrypted_value

    @property
    def source(self):
        return self._source

    @property
    def context(self):
        return self._context

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
        unencrypted_text = json.dumps({
            'id': self._pagination_id,
            'source': self._source,
            'context': self._context,
            'start': self._inclusive_start,
            'end': self._exclusive_end
        })
        return SneakyKipper('pagination').encrypt(unencrypted_text, {'username': self._username})
