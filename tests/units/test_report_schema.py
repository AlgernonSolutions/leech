import pytest

from toll_booth.alg_obj.posts.report_schema import ReportSchemaEntry, ReportSchema


@pytest.mark.report_schema
class TestReportSchema:
    def test_report_args_get(self, id_source):
        from toll_booth.alg_tasks.rivers.tasks.posts.email_tasks import _get_date
        kwargs = {'id_source': id_source, 'get_date': _get_date}
        schema = ReportSchema.get()
        report_args = schema.get_report_args(**kwargs)
        print()

    def test_report_schema_get(self):
        schema = ReportSchema.get()
        assert isinstance(schema, ReportSchema)

    def test_report_schema_entry_get(self):
        schema = ReportSchema.get()
        schema_entry = schema['Caseload Report']
        assert isinstance(schema_entry, ReportSchemaEntry)
        assert schema_entry.report_name == 'Caseload Report'

    def test_report_schema_population(self, id_source):
        from toll_booth.alg_tasks.rivers.tasks.posts.email_tasks import _get_date
        kwargs = {'id_source': id_source, 'get_date': _get_date}
        schema = ReportSchema.get()
        populated_schema = schema(**kwargs)
        print()
