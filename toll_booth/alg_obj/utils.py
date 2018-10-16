def convert_credible_datetime_to_gremlin(credible_datetime, is_utc=False):
    from pytz import timezone
    utc_format = '%Y-%m-%dT%H:%M:%S%z'
    credible_string = credible_datetime.strftime(utc_format)
    if not is_utc:
        return credible_string
    naive_datetime = credible_datetime.replace(tzinfo=None)
    utc_datetime = timezone('UTC').localize(naive_datetime)
    return utc_datetime.strftime(utc_format)


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
