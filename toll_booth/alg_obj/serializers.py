import json
from datetime import datetime
from decimal import Decimal

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
            return {'_alg_class': 'datetime', 'value': obj.timestamp()}
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
            return datetime.fromtimestamp(obj_value)
        if alg_class == 'decimal':
            return Decimal(obj_value)
        if alg_class in alg_classes:
            test = alg_classes[alg_class]
            alg_obj = test.from_json(obj_value)
            return alg_obj
        return obj
