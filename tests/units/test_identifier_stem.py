from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


object_properties = {
    'property_1': 1,
    'property_2': 'squids'
}
object_type = 'TestVertex'
expected_string = '#vertex#TestVertex#{"property_1": 1, "property_2": "squids"}#'
for_dynamo_expected = '#SOURCE#vertex#TestVertex#{"property_1": 1, "property_2": "squids"}#'
for_extractor_expected = object_properties.copy()
for_extractor_expected.update({
    'graph_type': 'vertex',
    'object_type': 'TestVertex'
})


class TestIdentifierStem:
    @classmethod
    def _construct(cls):
        return IdentifierStem('vertex', object_type, object_properties)

    def test_construction(self):
        identifier_stem = IdentifierStem('vertex', object_type, object_properties)
        assert isinstance(identifier_stem, IdentifierStem)

    def test_to_string(self):
        identifier_stem = self._construct()
        assert str(identifier_stem) == expected_string

    def test_to_dynamo(self):
        identifier_stem = self._construct()
        assert identifier_stem.for_dynamo == for_dynamo_expected

    def test_to_extractor(self):
        identifier_stem = self._construct()
        assert identifier_stem.for_extractor == for_extractor_expected

    def test_from_raw(self):
        identifier_stem = self._construct()
        from_raw = IdentifierStem.from_raw(expected_string)
        assert isinstance(from_raw, IdentifierStem)
        assert str(from_raw) == str(identifier_stem)
