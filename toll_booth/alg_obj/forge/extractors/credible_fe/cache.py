class CachedEmployeeIds:
    def __init__(self, emp_ids=None):
        if not emp_ids:
            emp_ids = {}
        self._promises = {}
        self._names = {}
        self._emp_ids = emp_ids

    @property
    def emp_ids(self):
        return self._emp_ids

    def add_emp_id(self, last_name, first_initial, emp_id):
        full_name = self._build_name(last_name, first_initial)
        self._emp_ids[full_name] = emp_id

    def get_emp_id(self, last_name, first_initial, timestamp):
        full_name = self._build_name(last_name, first_initial)
        emp_id = self._emp_ids.get(full_name, None)
        if emp_id == 'working':
            self._promises[timestamp] = full_name
            return PromiseToken(timestamp, self.fill_promise)
        return emp_id

    def mark_emp_id_working(self, last_name, first_initial, timestamp):
        full_name = self._build_name(last_name, first_initial)
        self._emp_ids[full_name] = 'working'
        self._promises[timestamp] = full_name

    def fill_promise(self, timestamp):
        promised_name = self._promises[timestamp]
        emp_id = self._emp_ids[promised_name]
        del(self._promises[timestamp])
        return emp_id

    @classmethod
    def _build_name(cls, last_name, first_initial):
        return f'{last_name}, {first_initial}'


class PromiseToken:
    def __init__(self, timestamp, callback):
        self._timestamp = timestamp
        self._callback = callback

    def __int__(self):
        try:
            return int(self._callback(self._timestamp))
        except ValueError:
            print()
