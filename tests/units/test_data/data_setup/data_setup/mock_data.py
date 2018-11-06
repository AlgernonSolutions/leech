from datetime import datetime

from mock import MagicMock

from tests.units.test_data.data_setup.schema_setup.schema_entry import MockVertexSchemaEntry


class MockExtractData:
    pretend_data = {
        'String': 'fake data',
        'Integer': 1010101,
        'DateTime': datetime.now()
    }

    @classmethod
    def get(cls, context):
        fake_data = MagicMock()
        faked_data = cls._generate_extracted_data(context)
        fake_data.extract.return_value = faked_data
        fake_data.extracted_data = faked_data
        return fake_data

    @classmethod
    def _generate_extracted_data(cls, context):
        schema_entry = MockVertexSchemaEntry.get(context)
        data_return = cls._generate_ruled_data(schema_entry)
        data_return['source'] = [cls.generate_source_data(schema_entry)]
        provided_extracted_data = getattr(context, 'extracted_data', {})
        for entry_name, data_entry in provided_extracted_data.items():
            data_return[entry_name] = data_entry
        malformation = getattr(context, 'malformed', None)
        if not malformation:
            return data_return
        if malformation == 'non-unique':
            data_return['source'] = [
                {'generic data': 'for a generic people'},
                {'malformed_data': 'because you can trust no one'}
            ]
        if malformation == 'missing':
            data_return['source'] = [{}]
        return data_return

    @classmethod
    def generate_source_data(cls, schema_entry):
        source_data = {}
        for entry_property_name, entry_property in schema_entry.entry_properties.items():
            source_data[entry_property_name] = cls.pretend_data[entry_property.property_data_type]
        return source_data

    @classmethod
    def _generate_ruled_data(cls, schema_entry):
        ruled_data = {}
        for rule_entry in schema_entry.rules.linking_rules:
            for entry in rule_entry.rules:
                specifiers = entry.target_specifiers
                for specifier in specifiers:
                    ruled_data[specifier.specifier_name] = [{
                        x: 'mocked object information' for x in specifier.extracted_properties}]
        return ruled_data
