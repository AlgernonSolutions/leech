import random
from datetime import datetime

from tests.units.test_data.data_setup.schema_setup.schema_entry import MockVertexSchemaEntry

mock_credible_data = {
    'String': {
        'data': 'made up string information for credible',
        'credible_type': 'string'
    },
    'Integer': {
        'data': 10101,
        'credible_type': 'short'
    },
    'DateTime': {
        'data': datetime.now().isoformat(),
        'credible_type': 'dateTime'
    }
}


class MockCredibleWS:
    def __init__(self, mocked_data):
        self._mocked_data = mocked_data

    @classmethod
    def get_for_monitor(cls, context):
        mock_data = [{'max': context.remote_maximum_value, 'min': context.remote_maximum_value - 10}]
        return cls(mock_data).as_requests_content

    @staticmethod
    def _build_table_entry(data_line):
        data_entries = []
        for entry in data_line:
            data_entries.append(f'<{entry.retrieve("data_name")}>{entry.retrieve("data_value")}</{entry.retrieve("data_name")}>')
        return ''.join(data_entries)

    def _build_returned_tables(self):
        returned_tables = []
        row_order = 0
        for data_line in self._mocked_data:
            returned_tables.append(f'''
                        <Table1 diffgr:id="Table{row_order+1}" msdata:rowOrder="{row_order}">
                        \r\n        {self._build_table_entry(data_line)}
                        \r\n      </Table{row_order+1}>
                    ''')
            row_order += 1
        return ''.join(returned_tables)

    def _build_element_definition(self):
        elements = []
        for data_line in self._mocked_data:
            for entry in data_line:
                elements.append(
                    f'<xs:element name="{entry.retrieve("data_name")}" type="xs:{entry.retrieve("data_type")}" minOccurs="0" />'
                )
            return ''.join(elements)

    @property
    def content(self):
        return self.as_requests_content

    @property
    def as_requests_content(self):
        return f'''<?xml version="1.0" encoding="utf-8"?>
            \r\n<DataSet xmlns="https://www.crediblebh.com/">
            \r\n  <xs:schema id="NewDataSet" xmlns="" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:msdata="urn:schemas-microsoft-com:xml-msdata">
            \r\n    <xs:element name="NewDataSet" msdata:IsDataSet="true" msdata:UseCurrentLocale="true">
            \r\n      <xs:complexType>
            \r\n        <xs:choice minOccurs="0" maxOccurs="unbounded">
            \r\n          <xs:element name="Table">
            \r\n            <xs:complexType>
            \r\n              <xs:sequence>
            \r\n                <xs:element name="emp_id" type="xs:short" minOccurs="0" />
            \r\n              </xs:sequence>
            \r\n            </xs:complexType>
            \r\n          </xs:element>
            \r\n          <xs:element name="Table1">
            \r\n            <xs:complexType>
            \r\n              <xs:sequence>
            \r\n                {self._build_element_definition()}
            \r\n              </xs:sequence>
            \r\n            </xs:complexType>
            \r\n          </xs:element>
            \r\n        </xs:choice>
            \r\n      </xs:complexType>
            \r\n    </xs:element>
            \r\n  </xs:schema>
            \r\n  <diffgr:diffgram xmlns:msdata="urn:schemas-microsoft-com:xml-msdata" xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1">
            \r\n    <NewDataSet xmlns="">
            \r\n        <Table diffgr:id="Table1" msdata:rowOrder="0">
            \r\n            <emp_id>3133</emp_id>
            \r\n        </Table>
            \r\n      {self._build_returned_tables()}
            \r\n    </NewDataSet>
            \r\n  </diffgr:diffgram>
            \r\n</DataSet>
        '''


class MockCredibleWsGenerator:
    @classmethod
    def generate_single_entry(cls):
        return MockCredibleWS([
            cls._generate_mock_entry()
        ])

    @classmethod
    def get_for_max_min(cls, context):
        entry = [
            {
                'data_name': 'max',
                'data_type': 'short',
                'data_value': int(context.remote_maximum_value)
            },
            {
                'data_name': 'min',
                'data_type': 'short',
                'data_value': int(context.remote_maximum_value - 10)
            }
        ]
        return MockCredibleWS([entry])

    @classmethod
    def get_for_extractor(cls, context):
        schema_entry = MockVertexSchemaEntry.get(context)
        entry_properties = schema_entry.entry_properties
        expected_return = [MockCredibleWS([cls._derive_for_query(entry_properties)])]
        expected_return.extend(cls._derive_for_ruled_targets(context, schema_entry))
        return expected_return

    @classmethod
    def _generate_mock_entry(cls):
        columns = [1, 2, 3, 4, 5, 6]
        data_type = [('short', 1001), ('string', 'some made up string')]
        num_columns = random.choice(columns)
        entry = []
        for x in range(num_columns):
            data_choice = random.choice(data_type)
            entry.append({
                'data_name': f'{"mock_data" + str(x)}',
                'data_type': data_choice[0],
                'data_value': data_choice[1]
            })
        return entry

    @classmethod
    def _find_extraction_profile(cls, context):
        extraction_method = context.active_params['method']
        schema_entry = MockVertexSchemaEntry.get(context)
        for extraction_type, extraction_profile in schema_entry.extract.items():
            if extraction_type == extraction_method:
                return extraction_profile
        else:
            raise RuntimeError(f'could not find extraction profile for {extraction_method}')

    @classmethod
    def _derive_for_ruled_targets(cls, context, schema_entry):
        ruled_queries = []
        for linking_rule_set in schema_entry.rules.linking_rules:
            for linking_rule in linking_rule_set.rules:
                target_type = linking_rule.target_type
                linked_schema_entry = MockVertexSchemaEntry.get(context, override_type=target_type)
                mock_report = MockCredibleWS([cls._derive_for_query(linked_schema_entry.entry_properties)])
                ruled_queries.append(mock_report)
        return ruled_queries

    @classmethod
    def _derive_for_query(cls, entry_properties):
        query = []
        for property_name, property_entry in entry_properties.items():
            data_type = property_entry.property_data_type
            credible_mapping = mock_credible_data[data_type]
            query.append({
                'data_name': property_name,
                'data_type': credible_mapping['credible_type'],
                'data_value': credible_mapping['data']
            })
        return query
