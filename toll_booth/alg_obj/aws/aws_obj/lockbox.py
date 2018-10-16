import datetime

import redis


class IndexConnection:
    def __init__(self, **kwargs):
        import os
        cache_url = kwargs.get('cache_url', os.getenv('CACHE_URL', 'unspecified'))
        cache_port = kwargs.get('cache_port', os.getenv('CACHE_PORT', '0'))
        db = kwargs.get('db', 0)
        client = redis.Redis(
            host=cache_url,
            port=cache_port,
            db=db,
            ssl=True)
        self._client = client

    @property
    def client(self):
        return self._client


class IndexDriver:
    def __init__(self, **kwargs):
        if 'index_connection' in kwargs:
            client = kwargs['index_connection'].client
        else:
            client = IndexConnection(**kwargs).client
        self._client = client

    @property
    def client(self):
        return self._client

    def query_index_max_min(self, index_name):
        max_min = {}
        pipeline = self._client.pipeline()
        pipeline.zrange(index_name, '-1', '-1', withscores=True)
        pipeline.zrange(index_name, '0', '0', withscores=True)
        results = pipeline.execute()
        for result in results[0]:
            max_min['max'] = result[0].decode(), int(result[1])
        for result in results[1]:
            max_min['min'] = result[0].decode(), int(result[1])
        return max_min

    def __enter__(self):
        pipeline = self._client.pipeline()
        self._pipeline = pipeline
        return pipeline

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type and not exc_val:
            self._pipeline.execute()
            return True
        raise (exc_type(exc_val))


class IndexKey:
    @classmethod
    def from_object_properties(cls, object_properties, index_schema):
        key_fields = index_schema.index_properties['key']
        index_name = index_schema.index_name
        index_values = ['index', index_name]
        for field_name in key_fields:
            field_value = str(object_properties[field_name]).lower()
            field_value = field_value.replace("'", '')
            index_values.append(field_value)
        if index_schema.index_type == 'sorted_set':
            index_values.append('range')
        return '.'.join(index_values)


class FuseLighter:
    def __init__(self, object_identifier, **kwargs):
        self._client = kwargs.get('index_connection', IndexConnection(**kwargs)).client
        self._object_type = object_identifier['object_type']
        self._id_source = object_identifier['id_source']
        self._id_name = object_identifier['id_name']

    def filter_working_ids(self, internal_ids):
        lazy_working_ids = []
        with self._client.pipeline() as pipeline:
            for internal_id in internal_ids:
                index_key = f'monitoring.{self._id_source}.{self._object_type}.{self._id_name}.{internal_id}'
                pipeline.exists(index_key)
            results = pipeline.execute()
            for internal_id in internal_ids:
                position = internal_ids.index(internal_id)
                if not results[position]:
                    lazy_working_ids.append(internal_id)
        return lazy_working_ids

    def check_if_id_working(self, internal_id):
        index_key = f'monitoring.{self._id_source}.{self._object_type}.{self._id_name}.{internal_id}'
        return self._client.exists(index_key)

    def mark_ids_as_working(self, internal_ids, ttl_hours=0, ttl_minutes=0, ttl_seconds=0):
        ttl = datetime.timedelta(hours=ttl_hours, minutes=ttl_minutes, seconds=ttl_seconds)
        if not ttl_hours and not ttl_minutes and not ttl_seconds:
            ttl = datetime.timedelta(hours=1)
        expiration_seconds = ttl.seconds
        with self._client.pipeline() as pipeline:
            for internal_id in internal_ids:
                index_key = f'monitoring.{self._id_source}.{self._object_type}.{self._id_name}.{internal_id}'
                pipeline.set(index_key, internal_id)
                pipeline.expire(index_key, expiration_seconds)
            pipeline.execute()

    def mark_id_as_working(self, internal_id, ttl_hours=0, ttl_minutes=0, ttl_seconds=0):
        ttl = datetime.timedelta(hours=ttl_hours, minutes=ttl_minutes, seconds=ttl_seconds)
        if not ttl_hours and not ttl_minutes and not ttl_seconds:
            ttl = datetime.timedelta(hours=1)
        expiration_seconds = ttl.seconds
        index_key = f'monitoring.{self._id_source}.{self._object_type}.{self._id_name}.{internal_id}'
        with self._client.pipeline() as pipeline:
            pipeline.set(index_key, internal_id)
            pipeline.expire(index_key, expiration_seconds)
            pipeline.execute()


class FuseFixer:
    def __init__(self, **kwargs):
        self._client = kwargs.get('index_connection', IndexConnection(**kwargs)).client
        self._identifier = kwargs.get('identifier', None)

    def reset_fuses(self):
        cursor = self._scan_and_reset_fuses()
        while cursor != 0:
            cursor = self._scan_and_reset_fuses(cursor)

    def _scan_and_reset_fuses(self, cursor=0):
        results = self._client.scan(cursor, match=self.scan_match)
        cursor = results[0]
        entries = tuple(results[1])
        if entries:
            self._client.delete(*entries)
        return cursor

    @property
    def scan_match(self):
        identifier = self._identifier
        index_stem = 'monitoring'
        match = f"{index_stem}.*"
        if identifier:
            match = f"{index_stem}.{identifier['id_source']}.{identifier['object_type']}.{identifier['id_name']}.*"
        return match
