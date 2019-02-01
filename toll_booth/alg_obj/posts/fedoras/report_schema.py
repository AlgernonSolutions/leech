import os

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.posts.fedoras.query_arguments import QueryArgument
from toll_booth.alg_obj.posts.fedoras.recipient_rules import ReportRecipientRules


class ReportQuery(AlgObject):
    def __init__(self, query_name, query_source, query_args):
        self._query_name = query_name
        self._query_source = query_source
        self._query_args = query_args

    @property
    def query_name(self):
        return self._query_name

    @property
    def query_source(self):
        return self._query_source

    @property
    def query_args(self):
        return self._query_args

    @classmethod
    def parse_from_schema_entry(cls, schema_entry):
        query_name = schema_entry['query_name']
        query_source = schema_entry['query_source']
        query_arg_data = schema_entry['query_args']
        query_args = {}
        for arg_name, arg_data in query_arg_data.items():
            query_args[arg_name] = QueryArgument.parse_from_schema_entry(arg_name, arg_data)
        return cls(query_name, query_source, query_args)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['query_name'], json_dict['query_source'], json_dict['query_args'])

    def __call__(self, *args, **kwargs):
        populated_args = {}
        for arg_name, query_arg in self._query_args.items():
            populated_args[arg_name] = query_arg(**kwargs)
        return {
            'query_source': self._query_source,
            'query_name': self._query_name,
            'query_args': populated_args
        }


class ReportSchemaEntry(AlgObject):
    def __init__(self, report_name, recipient_rules, queries):
        self._report_name = report_name
        self._recipient_rules = recipient_rules
        self._queries = queries

    @property
    def report_name(self):
        return self._report_name

    @classmethod
    def get(cls, report_name, **kwargs):
        schema = ReportSchema.get(**kwargs)
        return schema[report_name]

    @classmethod
    def parse_from_schema_entry(cls, schema_entry):
        report_name = schema_entry['report_name']
        queries = {}
        recipient_rules = {}
        for profile_name, query_profile in schema_entry.items():
            if profile_name == 'report_name':
                continue
            collected_queries = []
            profile_queries = query_profile.get('queries', [])
            profile_rules = query_profile.get('recipient_rules', {})
            for profile_query in profile_queries:
                report_query = ReportQuery.parse_from_schema_entry(profile_query)
                collected_queries.append(report_query)
            queries[profile_name] = collected_queries
            recipient_rules[profile_name] = ReportRecipientRules.parse_from_schema_entry(profile_rules)
        return cls(report_name, recipient_rules, queries)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['report_name'], json_dict['recipient_rules'], json_dict['queries'])

    def __call__(self, *args, **kwargs):
        populated_queries = {}
        for profile_name, queries in self._queries.items():
            profile_queries = {}
            for query in queries:
                profile_queries[query.query_name] = query(**kwargs)
            populated_queries[profile_name] = profile_queries
        return {self._report_name: populated_queries}


class ReportSchema(AlgObject):
    def __init__(self, entries):
        self._entries = entries

    @classmethod
    def get(cls, file_name=None, **kwargs):
        from toll_booth.alg_obj.aws.snakes.schema_snek import SchemaSnek
        if file_name is None:
            file_name = os.getenv('REPORT_SCHEMA_FILE_NAME', 'reports.json')
        snek = SchemaSnek(**kwargs)
        schema_file = snek.get_schema(file_name)
        entries = [ReportSchemaEntry.parse_from_schema_entry(x) for x in schema_file]
        return cls(entries)

    @classmethod
    def post(cls, schema_file_path, validation_file_path, **kwargs):
        from toll_booth.alg_obj.aws.snakes.schema_snek import SchemaSnek
        import jsonref
        from jsonschema import validate
        schema_file_name = kwargs.get('CONFIG_FILE', os.getenv('REPORT_SCHEMA_FILE_NAME', 'reports.json'))
        validation_file_name = kwargs.get('MASTER_CONFIG_FILE', os.getenv('REPORT_SCHEMA_FILE_NAME', 'master_reports.json'))
        snek = SchemaSnek(**kwargs)
        with open(schema_file_path) as schema_file, open(validation_file_path) as validation_file:
            working_schema = jsonref.load(schema_file)
            master_schema = jsonref.load(validation_file)
            validate(working_schema, master_schema)
            snek.put_schema(schema_file_path, schema_file_name)
            snek.put_schema(validation_file_path, validation_file_name)
            return cls([ReportSchemaEntry.parse_from_schema_entry(x) for x in working_schema])

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['entries'])

    def __getitem__(self, item):
        for entry in self._entries:
            if entry.report_name == item:
                return entry
        raise KeyError(item)

    def __call__(self, *args, **kwargs):
        populated_entries = [x(**kwargs) for x in self._entries]
        return populated_entries

    def get_report_args(self, **kwargs):
        report_args = {}
        populated_entries = self(**kwargs)
        for entry in populated_entries:
            for report_name, report_arg in entry.items():
                if report_name in report_args:
                    current_arg = report_args[report_name]
                    if not isinstance(current_arg, list):
                        report_args[report_name] = [current_arg]
                    report_args[report_name].append(report_arg)
                    continue
                report_args[report_name] = report_arg
        return report_args
