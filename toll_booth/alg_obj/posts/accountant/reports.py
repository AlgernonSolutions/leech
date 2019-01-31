class ReportTemplate:
    def __init__(self, template_id, template_entries):
        self._template_id = template_id
        self._template_entries = template_entries


class Report:
    def __init__(self, report_name, report_type, sheets=None):
        if not sheets:
            sheets = []
        self._report_name = report_name
        self._report_type = report_type
        self._sheets = sheets

    @property
    def report_name(self):
        return self._report_name

    @property
    def report_type(self):
        return self._report_type

    @property
    def sheets(self):
        return self._sheets

    def __iter__(self):
        return iter(self._sheets)
