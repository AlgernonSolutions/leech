import json
import logging

from toll_booth.alg_obj.aws.trident.graph_driver import TridentDriver
from toll_booth.alg_obj.aws.trident.trident_obj import TridentEdgeConnection
from toll_booth.alg_obj.graph.ogm.generator import CommandGenerator, VertexCommandGenerator, EdgeCommandGenerator
from toll_booth.alg_obj.graph.ogm.pages import PaginationToken
from toll_booth.alg_obj.graph.schemata.schema import Schema


class Ogm:
    def __init__(self, **kwargs):
        self._schema = kwargs.get('schema', Schema.retrieve(**kwargs))
        self._trident_driver = kwargs.get('trident_driver', TridentDriver())

    def execute(self, query):
        results = self._trident_driver.execute(query, False)
        return results

    def graph_objects(self, vertexes=None, edges=None):
        logging.info(f'')
        if not vertexes:
            vertexes = []
        if not edges:
            edges = []
        vertex_commands = {self._generate_vertex_command(x) for x in vertexes}
        edge_commands = {self._generate_edge_command(x) for x in edges}
        logging.info(f'generated graph commands: {vertex_commands, edge_commands} for vertexes/edges: {vertexes}/{edges}')
        return self._graph_objects(vertex_commands, edge_commands)

    def graph_object(self, object_entry):
        vertex_commands = set()
        edge_commands = set()
        vertex_commands.add(self._generate_vertex_command(object_entry['source']))
        for vertex in object_entry['others']:
            vertex_commands.add(self._generate_vertex_command(vertex))
        for edge in object_entry['edges']:
            edge_commands.add(self._generate_edge_command(edge))
        logging.info(f'generated graph commands: {vertex_commands, edge_commands} for potential object: {object_entry}')
        return self._graph_objects(vertex_commands, edge_commands)

    def _generate_vertex_command(self, potential_vertex):
        schema_entry = self._schema.get(potential_vertex.object_type)
        command_generator = VertexCommandGenerator(schema_entry)
        graph_command = command_generator.create_command(potential_vertex)
        return graph_command

    def _generate_edge_command(self, potential_edge):
        schema_entry = self._schema.get(potential_edge.object_type)
        command_generator = EdgeCommandGenerator(schema_entry)
        graph_command = command_generator.create_command(potential_edge)
        return graph_command

    def _graph_objects(self, vertex_commands, edge_commands):
        for command in vertex_commands:
            self._trident_driver.execute(command, False)
        for command in edge_commands:
            self._trident_driver.execute(command, False)
        logging.info(f'submitted graph commands: {vertex_commands, edge_commands}')
        return


class OgmReader:
    def __init__(self, trident_driver=None):
        if not trident_driver:
            trident_driver = TridentDriver()
        self._trident_driver = trident_driver

    def execute(self, query):
        results = self._trident_driver.execute(query, True)
        return results

    def get_vertex(self, source, function_args, **kwargs):
        internal_id = function_args.get('internal_id', source.get('internal_id'))
        return self._get_vertex_by_internal_id(internal_id)

    def get_vertex_properties(self,  source, function_args, **kwargs):
        name_filters = function_args.get('property_names', [])
        filter_string = ','.join('"{0}"'.format(w) for w in name_filters)
        internal_id = source.get('internal_id', function_args.get('internal_id', kwargs.get('internal_id', None)))
        if not internal_id:
            raise NotImplementedError(
                f'requested to get vertex properties, but no internal_id present, '
                f'source: {source}, function_args: {function_args}, kwargs: {kwargs}')
        query = f'g.V("{internal_id}").propertyMap([{filter_string}])'
        results = self._trident_driver.execute(query, True)
        if not results:
            return []
        return [y[0] for x, y in results[0].items()]

    def find_vertexes(self, source, function_args, **kwargs):
        internal_id = function_args.get('internal_id', source.get('internal_id'))
        identifier_stem = function_args.get('identifier_stem', source.get('identifier_stem'))
        sid_value = function_args.get('sid_value', source.get('sid_value'))
        vertex_properties = function_args.get('vertex_properties', source.get('vertex_properties'))
        if internal_id:
            vertex = self._get_vertex_by_internal_id(internal_id)
            return [vertex]
        return self._find_vertexes_by_identifiers(identifier_stem, sid_value, vertex_properties)

    def get_edge_connection(self, source, function_args, **kwargs):
        token_json = {
            'username': kwargs['username'],
            'token': function_args.get('token', None),
            'context': function_args,
            'source': source,
            'page_size': function_args.get('page_size', 10)
        }
        token = function_args.get('pagination_token', PaginationToken.from_json(token_json))
        internal_id = token.source.get('internal_id', token.context.get('internal_id'))
        edges, more = self._get_connected_edges(internal_id, token, function_args)
        token.increment()
        return TridentEdgeConnection(edges, token, more)

    def _find_vertexes_by_identifiers(self, identifier_stem=None, sid_value=None, vertex_properties=None):
        if not vertex_properties:
            vertex_properties = {}
        query = f'g.V()'
        if identifier_stem:
            query += f".has('identifier_stem', '{identifier_stem}')"
        if sid_value:
            query += f".has('sid_value', '{sid_value}')"
        if vertex_properties:
            property_queries = ''.join([self._generate_property_query(x, y) for x, y in vertex_properties.items()])
            query += property_queries
        return self._trident_driver.execute(query, read_only=True)

    def _get_vertex_by_internal_id(self, internal_id):
        query = f'g.V("{internal_id}")'
        results = self._trident_driver.execute(query, True)
        for result in results:
            return result

    def _get_connected_edges(self, internal_id, pagination_token, function_args):
        edge_labels = function_args.get('edge_labels', [])
        inclusive_start = pagination_token.inclusive_start
        exclusive_end = pagination_token.exclusive_end
        edge_label_string = ', '.join(edge_labels)
        query = f'g.V("{internal_id}")' \
                f'.bothE({edge_label_string})' \
                f'.range({inclusive_start}, {exclusive_end+1})'
        edges = self._trident_driver.execute(query, True)
        returned_edges = edges[0:(exclusive_end-inclusive_start)]
        more = len(edges) > len(returned_edges)
        return self.sort_edges(internal_id, returned_edges), more

    @classmethod
    def _generate_property_query(cls, property_name, property_value):
        return f".has('{property_name}', '{property_value}')"


    @classmethod
    def sort_edges(cls, internal_id, unsorted_edges):
        edges = {
            'inbound': [],
            'outbound': [],
            'all': []
        }
        for edge in unsorted_edges:
            edge_type = 'inbound'
            if edge.in_id == internal_id:
                edge_type = 'outbound'
            edges[edge_type].append(edge)
            edges['all'].append(edge)
        return edges

    @classmethod
    def calculate_neighbors(cls, out, get_edges):
        if out and get_edges:
            return 'outE()'
        if get_edges:
            return 'inE()'
        if out:
            return 'out()'
        return 'in()'
