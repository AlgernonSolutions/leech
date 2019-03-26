from toll_booth.alg_obj import AlgObject


class DcdbhDocumentationEntry(AlgObject):
    def __init__(self, goal, objective, intervention, documentation):
        self._goal = goal
        self._objective = objective
        self._intervention = intervention
        self._documentation = documentation

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['goal'], json_dict['objective'], json_dict['intervention'], json_dict['documentation'])

    @property
    def goal(self):
        return self._goal

    @property
    def objective(self):
        return self._objective

    @property
    def intervention(self):
        return self._intervention

    @property
    def documentation(self):
        return self._documentation


class DcdbhDocumentation(AlgObject):
    def __init__(self, entries=None, response=None):
        if not entries:
            entries = []
        self._entries = entries
        self._response = response

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict.get('entries'), json_dict.get('response'))

    @property
    def entries(self):
        return self._entries

    @property
    def response(self):
        return self._response

    def add_entry(self, documentation_entry):
        self._entries.append(documentation_entry)

    def set_response(self, response):
        self._response = response


class DcdbhEncounter(AlgObject):
    def __init__(self, clientvisit_id, client_id, emp_id, diagnosis, rev_timein, rev_timeout, documentation):
        self._clientvisit_id = clientvisit_id
        self._client_id = client_id
        self._emp_id = emp_id
        self._diagnosis = diagnosis
        self._rev_timein = rev_timein
        self._rev_timeout = rev_timeout
        self._documentation = documentation

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['clientvisit_id'], json_dict['client_id'], json_dict['emp_id'], json_dict['diagnosis'],
            json_dict['rev_timein'], json_dict['rev_timeout'], json_dict['documentation']
        )

    @property
    def clientvisit_id(self):
        return self._clientvisit_id

    @property
    def client_id(self):
        return self._client_id

    @property
    def emp_id(self):
        return self._emp_id

    @property
    def diagnosis(self):
        return self._diagnosis

    @property
    def rev_timein(self):
        return self._rev_timein

    @property
    def rev_timeout(self):
        return self._rev_timeout

    @property
    def documentation(self):
        return self._documentation


interesting_url = '/services/lookups_service.asmx/GetVisitDocVersions'
