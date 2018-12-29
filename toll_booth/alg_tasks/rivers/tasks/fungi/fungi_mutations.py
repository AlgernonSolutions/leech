import re
from decimal import Decimal, InvalidOperation


def split_entry(field_value):
    non_numeric_inside = re.compile('(?P<outside>[\w\s]+?)\s*\((?P<inside>(?=[a-zA-Z\s])[\w\s\d]+)\)')
    numeric_inside = re.compile('(?P<outside>[\w\s]+?)\s*\((?P<inside>[\d]+)\)')
    no_parenthesis_number = re.compile('^((?![()])\d)*$')
    has_numeric_inside = numeric_inside.search(field_value)
    has_non_numeric_inside = non_numeric_inside.search(field_value)
    is_just_number = no_parenthesis_number.search(field_value) is not None
    if has_numeric_inside:
        id_type = has_numeric_inside.group('outside')
        id_value = has_numeric_inside.group('inside')
        return Decimal(id_value), id_type
    if has_non_numeric_inside:
        id_value = has_non_numeric_inside.group('outside')
        id_type = has_non_numeric_inside.group('inside')
        try:
            return Decimal(id_value), id_type
        except InvalidOperation:
            return id_type, id_value
    if is_just_number:
        return Decimal(field_value), None
    return field_value, None


def split_record_id(cls, field_value, **kwargs):
    record_id, record_type = cls._split_entry(field_value)
    return {'record_id': record_id, 'record_type': record_type}


def convert_datetime_utc(field_value):
    from toll_booth.alg_obj.utils import convert_credible_fe_datetime_to_python
    return convert_credible_fe_datetime_to_python(field_value, True)
