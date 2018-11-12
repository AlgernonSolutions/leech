from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry


class Spore:
    def __init__(self, identifier_stem, id_value):
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        self._identifier_stem = identifier_stem
        self._id_value = id_value

    def propagate(self):
        schema_entry = SchemaVertexEntry.get(self._identifier_stem.object_type)
        for linking_rule in schema_entry.rules.linking_rules:
            print()
