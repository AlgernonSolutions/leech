from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaEntry


class CommandGenerator:
    def __init__(self, schema_entry):
        self._schema_entry = schema_entry
        self._indexes = schema_entry.indexes

    @classmethod
    def get_for_obj_type(cls, object_type):
        schema_entry = SchemaEntry.get(object_type)
        if hasattr(schema_entry, 'edge_label'):
            return EdgeCommandGenerator(schema_entry)
        return VertexCommandGenerator(schema_entry)

    def _derive_object_properties(self, graph_object):
        property_commands = []
        if graph_object.internal_id:
            property_commands.append(f".property(id, '{graph_object.internal_id}')")
        for property_name, property_value in graph_object.object_properties.items():
            if property_value is None:
                continue
            if hasattr(property_value, 'is_missing'):
                continue
            property_commands.append(
                f".property('{property_name}', {self._derive_property_value(property_name, property_value)})")
        return ''.join(property_commands)

    def _derive_property_value(self, property_name, property_value):
        property_data_type = self._schema_entry.entry_properties[property_name].property_data_type
        if property_data_type in ['String', 'DateTime']:
            property_value = f"'{property_value}'"
        return property_value

    def _derive_object_check(self, potential_object, is_edge=False):
        object_type = 'V'
        if is_edge:
            object_type = 'E'
        internal_id = potential_object.internal_id
        if internal_id:
            value = f"g.{object_type}('{internal_id}')"
            return value
        checked_properties = []
        for property_name, property_value in potential_object.object_properties.items():
            checked_properties.append(
                f".has('{property_name}', {self._derive_property_value(property_name, property_value)})")
        return f"""
            g.{object_type}().hasLabel('{potential_object.object_type}').hasLabel('stub'){''.join(checked_properties)}
        """

    @classmethod
    def _derive_add_object_start(cls, potential_object, is_edge=False):
        object_type = 'V'
        if is_edge:
            object_type = 'E'
        return f"add{object_type}('{potential_object.graphed_object_type}')"

    @staticmethod
    def prepare_properties(object_properties):
        prepared_properties = {}
        for object_property_name, object_property in object_properties.items():
            try:
                object_property = object_property.replace("\\", r"\\")
                object_property = object_property.replace("'", r"\'")
                object_property = "'%s'" % str(object_property)
            except AttributeError:
                pass
            prepared_properties[object_property_name] = object_property
        return prepared_properties


class VertexCommandGenerator(CommandGenerator):
    def __init__(self, schema_entry):
        super().__init__(schema_entry)
        self._indexes = schema_entry.indexes

    def create_command(self, potential_vertex):
        return f"{self._derive_vertex_check(potential_vertex)}.fold().coalesce(unfold(), " \
               f"{self._derive_add_vertex(potential_vertex)})"

    def _derive_vertex_check(self, potential_vertex):
        value = self._derive_object_check(potential_vertex)
        return value

    def _derive_add_vertex(self, potential_vertex):
        return f"{self._derive_add_vertex_start(potential_vertex)}{self._derive_object_properties(potential_vertex)}"

    def _derive_add_vertex_start(self, potential_vertex):
        return self._derive_add_object_start(potential_vertex)


class EdgeCommandGenerator(CommandGenerator):
    def __init__(self, schema_entry):
        super().__init__(schema_entry)
        self._rules = schema_entry.rules

    def create_command(self, potential_edge):
        import re
        command = f"{self._derive_edge_check(potential_edge)}.fold().coalesce(unfold(), " \
                  f"{self._add_edge_start(potential_edge)}." \
                  f"from(g.V('{potential_edge.from_object}')).to(g.V('{potential_edge.to_object}'))" \
                  f"{self._derive_edge_properties(potential_edge)})"
        command = re.sub('\s+', ' ', command)
        return command

    def _derive_edge_check(self, potential_edge):
        value = self._derive_object_check(potential_edge, True)
        return value

    def _derive_edge_properties(self, potential_edge):
        value = self._derive_object_properties(potential_edge)
        return value

    def _add_edge_start(self, potential_edge):
        value = self._derive_add_object_start(potential_edge, True)
        return value
