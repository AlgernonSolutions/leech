class MalformedSchemaException(Exception):
    def __init__(self, message):
        super().__init__(message)


class SchemaMissingVertex(MalformedSchemaException):
    def __init__(self, bucket_name, file_key):
        message = 'malformed schema json passed in, drawn from %s with key %s, does not contain a vertex entry'
        message = message % (bucket_name, file_key)
        super().__init__(message)


class SchemaMissingEdge(MalformedSchemaException):
    def __init__(self, bucket_name, file_key):
        message = 'malformed schema json passed in, drawn from %s with key %s, does not contain a edge entry'
        message = message % (bucket_name, file_key)
        super().__init__(message)


class MissingSchemaEntryProperty(MalformedSchemaException):
    def __init__(self, entry_name):
        message = 'cannot create schema entry for %s, no properties field declared, if object has no properties, ' \
                  'indicate with {}'
        message = message % entry_name
        super().__init__(message)


class InvalidSchemaPropertyType(MalformedSchemaException):
    def __init__(self, property_name, property_data_type, accepted_data_types):
        message = 'cannot create property with name %s, data_type %s, data_type is not of accepted type, ' \
                  'accepted types are: %s'
        message = message % (property_name, property_data_type, accepted_data_types)
        super().__init__(message)


class GraphConstructionException(Exception):
    def __init__(self, message):
        super().__init__(message)


class RadiantGraphGraftException(Exception):
    def __init__(self, message):
        super().__init__(message)


class ChangeDetailConstructionException(Exception):
    def __init__(self, report_entry):
        message = 'unable to parse change_detail from given report entry: %s' % report_entry
        super().__init__(message)


class ExternalIdConstructionException(Exception):
    def __init__(self, *args):
        strung_args = [str(x) for x in args]
        message = 'unable to construct external id from these arguments: %s' % (','.join(strung_args))
        super().__init__(message)


class InvalidExtractionException(Exception):
    def __init__(self, message):
        base_message = 'while extracting information, an exception occurred'
        super().__init__(base_message + '//' + message)


class InvalidExtractionMultipleSourceException(InvalidExtractionException):
    def __init__(self, extraction_source, monitor_order):
        message = 'the attempted extraction returned multiple responses for a single monitoring order. ' \
                  'this generally means that the monitored object is either not uniquely keyed, or you are ' \
                  'monitoring the wrong value, extraction_source: %s, monitor_order: %s' % (
                      extraction_source, monitor_order)
        super().__init__(message)
