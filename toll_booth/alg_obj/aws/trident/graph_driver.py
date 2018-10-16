from toll_booth.alg_obj.aws.trident.connections import TridentNotary


class TridentDriver:
    def __init__(self, **kwargs):
        self._read_notary = kwargs.get('read_notary', TridentNotary.get_for_reader(**kwargs))
        self._write_notary = kwargs.get('write_notary', TridentNotary.get_for_writer(**kwargs))
        self._batch_mode = False

    def get(self, internal_id):
        command = "g.V('%s')" % internal_id
        return self.execute(command, True)

    def execute(self, query_text, read_only=False):
        if self._batch_mode is True:
            self._batch_commands.append(query_text)
            return
        notary = self._write_notary
        if read_only:
            notary = self._read_notary
        results = notary.send(query_text)
        return results

    def __enter__(self):
        self._batch_commands = []
        self._batch_mode = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type and not exc_val:
            self._batch_mode = False
            commands = ';'.join(self._batch_commands)
            self.execute(commands)
            return True
        raise (exc_type(exc_val))


class TridentScanner:
    def __init__(self, object_type, object_properties=None, is_edge=False, **kwargs):
        if not object_properties:
            object_properties = {}
        self._object_type = object_type
        self._trident_notary = kwargs.get('trident_notary', TridentNotary.get_for_reader(**kwargs))
        self._object_properties = object_properties
        object_identifier = 'V'
        if is_edge:
            object_identifier = 'E'
        self._object_identifier = object_identifier
        self._is_edge = is_edge
        self._segment_size = kwargs.get('segment_size', 50)

    def get_max_min_internal_id(self):
        query = self._build_max_min_query()
        min_query = query % 'decr'
        max_query = query % 'incr'
        min_id = self._trident_notary.send(min_query)
        max_id = self._trident_notary.send(max_query)
        return max_id[0], min_id[0]

    def full_scan(self):
        scan_results = []
        results, cursor = self._scan()
        scan_results.extend(results)
        while cursor != 0:
            results, cursor = self._scan(after_value=cursor)
            scan_results.extend(results)
        return scan_results

    def scan(self, cursor=0):
        results, cursor = self._scan(after_value=cursor)
        return cursor, results

    def _scan(self, after_value=0):
        scan_query = self._build_scan_query()
        next_value = after_value + self._segment_size
        query = scan_query % (after_value, next_value)
        results = self._trident_notary.send(query)
        if not results:
            next_value = 0
        return results, next_value

    def _build_max_min_query(self):
        query = f"g.{self._object_identifier}().hasLabel('{self._object_type}')" \
                f"{self._build_properties()}.order().by(id, %s).limit(1).id()"
        return query

    def _build_scan_query(self):
        query = f"g.{self._object_identifier}().hasLabel('{self._object_type}')" \
                f"{self._build_properties()}.order().by(id, incr).range(%s, %s)"
        return query

    def _build_properties(self):
        results = []
        for property_name, object_property in self._object_properties.items():
            results.append(f".has('{property_name}', '{object_property}')")
        return ''.join(results)
