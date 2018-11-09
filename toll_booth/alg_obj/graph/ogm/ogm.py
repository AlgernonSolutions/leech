import logging

from toll_booth.alg_obj.aws.trident.graph_driver import TridentDriver
from toll_booth.alg_obj.aws.trident.trident_obj import TridentEdgeConnection
from toll_booth.alg_obj.graph.ogm.generator import CommandGenerator
from toll_booth.alg_obj.graph.ogm.pages import PaginationToken


class Ogm:
    def __init__(self, **kwargs):
        self._trident_driver = kwargs.get('trident_driver', TridentDriver())

    def execute(self, query):
        results = self._trident_driver.execute(query, False)
        return results

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

    @classmethod
    def _generate_vertex_command(cls, potential_vertex):
        command_generator = CommandGenerator.get_for_obj_type(potential_vertex.object_type)
        graph_command = command_generator.create_command(potential_vertex)
        return graph_command

    @classmethod
    def _generate_edge_command(cls, potential_edge):
        command_generator = CommandGenerator.get_for_obj_type(potential_edge.object_type)
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
        query = f'g.V("{internal_id}")'
        results = self._trident_driver.execute(query, True)
        for result in results:
            return result

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

    def get_edge_connection(self, source, function_args, **kwargs):
        token_json = {
            'username': kwargs['username'],
            'token': function_args.get('token', None),
            'page_size': function_args.get('page_size', 10)
        }
        token = function_args.get('pagination_token', PaginationToken.from_json(token_json))
        internal_id = source['internal_id']
        edges, more = self._get_connected_edges(internal_id, token, function_args)
        token.increment()
        return TridentEdgeConnection(edges, token, more)

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
