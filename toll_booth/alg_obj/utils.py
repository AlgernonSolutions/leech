import collections
from datetime import datetime

from dateutil.parser import parse


def recursively_update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            d[k] = recursively_update(d.get(k, {}), v)
        elif k in d and isinstance(d[k], list):
            if v:
                try:
                    d[k].extend(v)
                except AttributeError:
                    d[k].append(v)
        else:
            d[k] = v
    return d


def convert_credible_fe_datetime_to_python(datetime_string, is_utc=False):
    from pytz import timezone
    python_datetime = parse(datetime_string)
    if not is_utc:
        return python_datetime
    naive_datetime = python_datetime.replace(tzinfo=None)
    utc_datetime = timezone('UTC').localize(naive_datetime)
    return utc_datetime


def convert_credible_datetime_to_gremlin(credible_datetime, is_utc=False):
    from pytz import timezone
    utc_format = '%Y-%m-%dT%H:%M:%S%z'
    credible_string = credible_datetime.strftime(utc_format)
    if not is_utc:
        return credible_string
    naive_datetime = credible_datetime.replace(tzinfo=None)
    utc_datetime = timezone('UTC').localize(naive_datetime)
    return utc_datetime.strftime(utc_format)


def convert_python_datetime_to_gremlin(python_datetime):
    from pytz import timezone
    gremlin_format = '%Y-%m-%dT%H:%M:%S%z'
    if isinstance(python_datetime, str):
        python_datetime = datetime.strptime(python_datetime, gremlin_format)
    if not python_datetime.tzinfo:
        naive_datetime = python_datetime.replace(tzinfo=None)
        utc_datetime = timezone('UTC').localize(naive_datetime)
        return utc_datetime.strftime(gremlin_format)
    return python_datetime.strftime(gremlin_format)


def correct_credible_datetime_tz(credible_datetime, is_utc=False):
    from pytz import timezone
    if not is_utc:
        return credible_datetime
    naive_datetime = credible_datetime.replace(tzinfo=None)
    utc_datetime = timezone('UTC').localize(naive_datetime)
    return utc_datetime


def recurse_subclasses(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in recurse_subclasses(c)])


def get_subclasses(cls):
    subclasses = recurse_subclasses(cls)
    return {str(x): x for x in subclasses}
