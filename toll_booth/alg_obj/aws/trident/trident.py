from toll_booth.alg_obj.utils import convert_credible_datetime_to_gremlin

trident_queries = {
    'getVertex': "g.V()"
                 ".hasId('{internal_id}')",
    'findExternalId': "g.V()"
                      ".hasLabel('ExternalId')"
                      ".has('id_source', '{id_source}')"
                      ".has('id_type', '{id_type}')"
                      ".has('id_value', {id_value})",
    'getExternalId': "g.V()"
                     ".hasLabel('ExternalId')"
                     ".hasId('{internal_id}')",
    'listExternalIds': "g.V()"
                       ".hasLabel('ExternalId')"
                       ".hasId({internal_ids})",
    'getExternalIds': "g.V()"
                      ".hasLabel('ExternalId')"
                      ".has('id_source', '{id_source}')"
                      ".has('id_type', '{id_type}')"
                      ".has('id_value', gte({low_id}))"
                      ".has('id_value', lte({hi_id}))",
    'getExternalIdValues': "g.V()"
                           ".hasLabel('ExternalId')"
                           ".has('id_source', '{id_source}')"
                           ".has('id_type', '{id_type}')"
                           ".has('id_value', gte({low_id}))"
                           ".has('id_value', lte({hi_id}))"
                           ".properties('id_value')",
    'pageExternalIdValues': "g.V()"
                            ".hasLabel('ExternalId')"
                            ".has('id_source', '{id_source}')"
                            ".has('id_type', '{id_type}')"
                            ".range({after}, {next})"
                            ".values('id_value')",
    'pageExternalIds': "g.V()"
                       ".hasLabel('ExternalId')"
                       ".has('id_source', '{id_source}')"
                       ".has('id_type', '{id_type}')"
                       ".order().by('id_value', incr)"
                       ".range({after}, {next})",
    'pageChangeLogEntries': "g.V()"
                            ".hasLabel('ChangeLogEntry')"
                            ".has('id_source', '{id_source}')"
                            ".order().by('changelog_id', incr)"
                            ".range({after}, {next})",
    'pageChangeDetails': "g.V()"
                         ".hasLabel('Change')"
                         ".has('id_source', '{id_source}')"
                         ".order().by(id, incr)"
                         ".range({after}, {next})",
    'chunkExternalIds': "g.V()"
                        ".hasLabel('ExternalId')"
                        ".has('id_source', '{id_source}')"
                        ".has('id_type', '{id_type}')"
                        ".has('id_value', gte({low_id}))"
                        ".has('id_value', lte({hi_id}))"
                        ".range({after}, {next})",
    'singleTypeExternalIds': "g.V()"
                             ".hasLabel('ExternalId')"
                             ".has('id_source', '{id_source}')"
                             ".has('id_type', '{id_type}')",
    'outbound_edges': "g.V()"
                      ".hasId('{internal_id}')"
                      ".outE()",
    'inbound_edges': "g.V()"
                     ".hasId('{internal_id}')"
                     ".inE()",
    'GenericEdge.internal_id': "",
    'GenericEdge.label': "g.E()"
                         ".hasId('{internal_id}')"
                         ".label()",
    'GenericEdge.in_vertex': "g.E()"
                             ".hasId('{internal_id}')"
                             ".inV()",
    'GenericEdge.out_vertex': "g.E()"
                              ".hasId('{internal_id}')"
                              ".outV()",
    'ExternalId.internal_id': "g.V()"
                              ".hasLabel('ExternalId')"
                              ".has('id_source', '{id_source}')"
                              ".has('id_type', '{id_type}')"
                              ".has('id_value', {id_value}).id()",
    'ExternalId.id_source': "g.V()"
                            ".hasLabel('ExternalId')"
                            ".hasId('{internal_id}')"
                            ".properties('id_source').value()",
    'ExternalId.id_type': "g.V()"
                          ".hasLabel('ExternalId')"
                          ".hasId('{internal_id}')"
                          ".properties('id_type').value()",
    'ExternalId.id_name': "g.V()"
                          ".hasLabel('ExternalId')"
                          ".hasId('{internal_id}')"
                          ".properties('id_name').value()",
    'ExternalId.id_value': "g.V()"
                           ".hasLabel('ExternalId')"
                           ".hasId('{internal_id}')"
                           ".properties('id_value').value()",
    'addExternalId': "g.addV('ExternalId')"
                     ".property(id, '{internal_id}')"
                     ".property('id_source', '{id_source}')"
                     ".property('id_type', '{id_type}')"
                     ".property('id_name', '{id_name}')"
                     ".property('id_value', {id_value})",
    'postChangeLogEntry': "dynamic",
    'postExternalId': "dynamic",
    'getExtIdMaxMin': "g.V()"
                      ".hasLabel('ExternalId')"
                      ".has('id_source', '{id_source}')"
                      ".has('id_type', '{id_type}')"
                      ".values('id_value').fold().project('max', 'min')"
                      ".by(max(local)).by(min(local))",
    'getChangeLogMaxMin': "g.V()"
                          ".hasLabel('ChangeLogEntry')"
                          ".has('id_source', '{id_source}')"
                          ".values('changelog_id').fold().project('max', 'min')"
                          ".by(max(local)).by(min(local))"
}


class TridentAdd:
    property_command = ".property('{property_name}', {property_value})"

    @property
    def base_command(self):
        return ''

    @property
    def properties(self):
        return []

    @property
    def command_text(self):
        from datetime import datetime
        property_commands = ''
        for added_property in self.properties:
            try:
                base_property_command = self.property_command.format(**added_property)
            except TypeError:
                property_value = added_property.value
                if isinstance(property_value, datetime):
                    property_value = convert_credible_datetime_to_gremlin(property_value)
                if isinstance(property_value, str):
                    property_value = "'%s'" % property_value
                added_property = {
                    'property_name': added_property.label,
                    'property_value': property_value
                }
                base_property_command = self.property_command.format(**added_property)
            property_commands = property_commands + base_property_command
        parameters = self.__dict__
        parameters['properties'] = property_commands
        base_command = self.base_command.format(**parameters)
        base_command = base_command.replace('\n', ' ')
        return base_command


class TridentEdgeAdd:
    base_command = "g.E('{_internal_id}')" \
                   ".fold()" \
                   ".coalesce(" \
                   "    unfold(), " \
                   "    g.addE('{_edge_label}')" \
                   "    .from(g.V('{_out_id}'))" \
                   "    .to(g.V('{_in_id}'))" \
                   "    .property(id, '{_internal_id}')" \
                   ")"

    def __init__(self, internal_id, edge_label, in_id, out_id, edge_properties=None):
        if not edge_properties:
            edge_properties = []
        self._internal_id = internal_id
        self._edge_label = edge_label
        self._in_id = in_id
        self._out_id = out_id
        self._edge_properties = edge_properties

    @property
    def internal_id(self):
        return self._internal_id

    @property
    def edge_label(self):
        return self._edge_label

    @property
    def in_id(self):
        return self._in_id

    @property
    def out_id(self):
        return self._out_id

    @property
    def properties(self):
        return self._edge_properties


class StubVertexAdd:
    _base_command = "g.V()" \
                    ".hasLabel('stub')" \
                    ".has('object_type', '{entry_name}')" \
                    "{filter}" \
                    ".fold()" \
                    ".coalesce(" \
                    "unfold(), addV('stub').property('object_type', '{entry_name}'){properties}" \
                    ")"
    _filter_command = ".has('{_property_name}', {_property_value})"
    _add_stub_base = "addV('stub').property('object_type', '{_entry_name}')"
    _property_command = ".property('{_property_name}', {_property_value})"

    def __init__(self, obj_type, object_properties):
        property_commands = []
        filter_commands = []
        for property_name, property_value in object_properties.items():
            property_text = {'_property_name': property_name, '_property_value': property_value}
            property_line = self._property_command.format(**property_text)
            filter_line = self._filter_command.format(**property_text)
            property_commands.append(property_line)
            filter_commands.append(filter_line)
        text = {
            'entry_name': obj_type,
            'properties': ''.join(property_commands),
            'filter': ''.join(filter_commands)
        }
        base_command = self._base_command.format(**text)
        self._command = base_command

    @property
    def command_text(self):
        return self._command


class DynamicVertexAdd:
    _base_command = "g.V('{_internal_id}')" \
                    ".fold()" \
                    ".coalesce(unfold(), addV('{_entry_name}').property(id,'{_internal_id}'){properties})"
    _property_command = ".property('{_property_name}', {_property_value})"

    def __init__(self, obj_name, internal_id, object_properties):
        property_commands = []
        for property_name, property_value in object_properties.items():
            property_text = {'_property_name': property_name, '_property_value': property_value}
            property_line = self._property_command.format(**property_text)
            property_commands.append(property_line)
        text = {'_entry_name': obj_name, '_internal_id': internal_id, 'properties': ''.join(property_commands)}
        base_command = self._base_command.format(**text)
        self._command = base_command

    @property
    def command_text(self):
        return self._command
