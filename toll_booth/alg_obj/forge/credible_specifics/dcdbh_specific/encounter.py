class DocumentationEntry:
    def __init__(self, entry_prompt, entry_response):
        self._entry_prompt = entry_prompt
        self._entry_response = entry_response

    @property
    def entry_prompt(self):
        return self._entry_prompt

    @property
    def entry_response(self):
        return self._entry_response


class DcdbhEncounter:
    def __init__(self, ):
        pass


interesting_url = '/services/lookups_service.asmx/GetVisitDocVersions'
