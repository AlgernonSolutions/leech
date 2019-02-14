class SchemaParer:
    @classmethod
    def parse(cls, json_schema):
        vertexes = {}
        edges = {}
        for json_vertex in json_schema['vertex']:
            vertex_entry = cls.parse_vertex_entry(json_vertex)
            vertexes[vertex_entry.vertex_name] = vertex_entry
        for json_edge in json_schema['edge']:
            edge_entry = cls.parse_edge_entry(json_edge)
            edges[edge_entry.edge_label] = edge_entry
        return vertexes, edges

    @classmethod
    def parse_edge_entry(cls, json_edge):
        from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaEdgeEntry

        edge_label = json_edge['edge_label']
        from_types, to_types = json_edge['from'], json_edge['to']
        indexes = cls.parse_indexes(json_edge['indexes'])
        internal_id_key = cls.parse_internal_id_key(json_edge['internal_id_key'])
        edge_properties = cls.parse_edge_properties(json_edge['edge_properties'])
        args = (edge_label, from_types, to_types, edge_properties, internal_id_key, indexes)
        return SchemaEdgeEntry(*args)

    @classmethod
    def parse_vertex_entry(cls, json_vertex):
        from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry
        vertex_name = json_vertex['vertex_name']
        vertex_properties = cls.parse_vertex_properties(json_vertex['vertex_properties'])
        internal_id_key = cls.parse_internal_id_key(json_vertex['internal_id_key'])
        identifier_stem = cls.parse_identifier_stem_key(json_vertex['identifier_stem'])
        indexes = cls.parse_indexes(json_vertex['indexes'])
        rules = cls.parse_vertex_rules(json_vertex['rules'])
        extract = cls.parse_extracts(json_vertex['extract'])
        args = (vertex_name, vertex_properties, internal_id_key, identifier_stem, indexes, rules, extract)
        return SchemaVertexEntry(*args)

    @classmethod
    def parse_extracts(cls, json_extracts):
        from toll_booth.alg_obj.graph.schemata.schema_entry import ExtractionInstruction
        extracts = {}
        for extract_entry in json_extracts:
            extraction_instructions = ExtractionInstruction.from_json(extract_entry)
            extracts[extraction_instructions.extraction_source] = extraction_instructions
        return extracts

    @classmethod
    def parse_indexes(cls, json_indexes):
        from toll_booth.alg_obj.graph.schemata.indexes import SchemaIndexEntry
        indexes = {}
        for index_entry in json_indexes:
            index = SchemaIndexEntry.from_json(index_entry)
            indexes[index.index_name] = index
        return indexes

    @classmethod
    def parse_vertex_rules(cls, json_rules):
        from toll_booth.alg_obj.graph.schemata.rules import VertexRules
        from toll_booth.alg_obj.graph.schemata.rules import VertexLinkRuleSet

        vertex_rules = VertexRules()
        linking_rules = json_rules.get('linking_rules', [])
        for rule_entry in linking_rules:
            rule_set = VertexLinkRuleSet(
                rule_entry['vertex_specifiers'],
                [cls.parse_vertex_rule(x, is_inbound=False) for x in rule_entry['inbound']],
                [cls.parse_vertex_rule(x, is_inbound=True) for x in rule_entry['inbound']]
            )
            vertex_rules.add_rule_set(rule_set)
        return vertex_rules

    @classmethod
    def parse_vertex_rule(cls, json_rule, is_inbound):
        from toll_booth.alg_obj.graph.schemata.rules import VertexLinkRuleEntry

        edge_type = json_rule['edge_type']
        target_type = json_rule['target_type']
        if_absent = json_rule['if_absent']
        json_specifiers = json_rule['target_specifiers']
        target_specifiers = [cls.parse_target_specifier(x) for x in json_specifiers]
        target_constants = json_rule['target_constants']
        args = (target_type, edge_type, target_constants, target_specifiers, if_absent, is_inbound)
        return VertexLinkRuleEntry(*args)

    @classmethod
    def parse_target_specifier(cls, json_target_specifier):
        from toll_booth.alg_obj.graph.schemata.rules import ExtractionSpecifier
        from toll_booth.alg_obj.graph.schemata.rules import SharedPropertySpecifier
        from toll_booth.alg_obj.graph.schemata.rules import FunctionSpecifier

        specifier_type = json_target_specifier['specifier_type']
        if specifier_type == 'function':
            return FunctionSpecifier.from_json(json_target_specifier)
        if specifier_type == 'shared_property':
            return SharedPropertySpecifier.from_json(json_target_specifier)
        if specifier_type == 'extraction':
            return ExtractionSpecifier.from_json(json_target_specifier)
        raise NotImplementedError('could not generate a target specifier from the provided schema')

    @classmethod
    def parse_vertex_properties(cls, json_properties):
        from toll_booth.alg_obj.graph.schemata.entry_property import SchemaPropertyEntry
        vertex_properties = {}
        for property_entry in json_properties:
            vertex_property = SchemaPropertyEntry.from_json(property_entry)
            vertex_properties[vertex_property.property_name] = vertex_property
        return vertex_properties

    @classmethod
    def parse_edge_properties(cls, json_properties):
        from toll_booth.alg_obj.graph.schemata.entry_property import EdgePropertyEntry

        edge_properties = {}
        for property_entry in json_properties:
            edge_property = EdgePropertyEntry.from_json(property_entry)
            edge_properties[edge_property.property_name] = edge_property
        return edge_properties

    @classmethod
    def parse_internal_id_key(cls, json_key):
        from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaInternalIdKey
        return SchemaInternalIdKey(json_key)

    @classmethod
    def parse_identifier_stem_key(cls, json_key):
        from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaIdentifierStem
        return SchemaIdentifierStem(json_key)
