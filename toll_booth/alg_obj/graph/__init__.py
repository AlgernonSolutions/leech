class InternalId:
    def __init__(self, internal_id):
        self._internal_id = internal_id

    @property
    def id_value(self):
        import hashlib

        return hashlib.md5(self._internal_id.encode('utf-8')).hexdigest()

    def __str__(self):
        return self.id_value
