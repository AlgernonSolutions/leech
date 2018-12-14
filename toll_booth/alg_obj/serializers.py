import json
from datetime import datetime
from decimal import Decimal

import pytz
from jsonref import JsonRef

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.utils import get_subclasses


class AlgEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, AlgObject):
            return {'_alg_class': str(obj.__class__), 'value': obj.to_json}
        if isinstance(obj, frozenset):
            list_value = [x for x in obj]
            return {'_alg_class': 'frozenset', 'value': list_value}
        if isinstance(obj, tuple):
            return {'_alg_class': 'tuple', 'value': obj}
        if isinstance(obj, datetime):
            if obj.tzinfo is None:
                return {'_alg_class': 'datetime', 'value': {'tz': None, 'timestamp': obj.timestamp()}}
            return {'_alg_class': 'datetime', 'value': {'tz': obj.tzinfo.zone, 'timestamp': obj.timestamp()}}
        if isinstance(obj, Decimal):
            return {'_alg_class': 'decimal', 'value': str(obj)}
        if isinstance(obj, JsonRef):
            subject = obj.__subject__
            return subject
        return super(AlgEncoder, self).default(obj)


class AlgDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    @staticmethod
    def object_hook(obj):
        alg_classes = get_subclasses(AlgObject)
        if '_alg_class' not in obj:
            return obj
        alg_class = obj['_alg_class']
        obj_value = obj['value']
        if alg_class == 'frozenset':
            return frozenset(x for x in obj_value)
        if alg_class == 'tuple':
            return tuple(x for x in obj_value)
        if alg_class == 'datetime':
            tz_info = obj_value['tz']
            timestamp = obj_value['timestamp']
            if tz_info is None:
                return datetime.fromtimestamp(timestamp)
            return datetime.fromtimestamp(timestamp, pytz.timezone(tz_info))
        if alg_class == 'decimal':
            return Decimal(obj_value)
        if alg_class in alg_classes:
            test = alg_classes[alg_class]
            alg_obj = test.from_json(obj_value)
            return alg_obj
        return obj


class GqlDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    @staticmethod
    def object_hook(obj):
        alg_classes = get_subclasses(AlgObject)
        if '_alg_class' not in obj:
            return obj
        alg_class = obj['_alg_class']
        obj_value = obj['value']
        if alg_class == 'frozenset':
            return frozenset(x for x in obj_value)
        if alg_class == 'tuple':
            return tuple(x for x in obj_value)
        if alg_class == 'datetime':
            return str(obj_value)
        if alg_class == 'decimal':
            return Decimal(obj_value)
        if alg_class in alg_classes:
            test = alg_classes[alg_class]
            alg_obj = test.from_json(obj_value)
            return alg_obj.to_gql
        return obj


class ExplosionDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    @staticmethod
    def object_hook(obj):
        if isinstance(obj, dict):
            if 'S' in obj:
                return str(obj['S'])
            if 'M' in obj:
                return obj['M']
            if 'BOOL' in obj:
                return bool(obj['BOOL'])
            if 'NULL' in obj:
                return None
            if 'N' in obj:
                return Decimal(obj['N'])
            if 'L' in obj:
                return list(obj['L'])
            return obj
        return obj
