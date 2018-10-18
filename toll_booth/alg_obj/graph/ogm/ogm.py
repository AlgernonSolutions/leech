from toll_booth.alg_obj.aws.aws_obj.lockbox import IndexDriver
from toll_booth.alg_obj.aws.trident.graph_driver import TridentDriver
from toll_booth.alg_obj.aws.trident.trident_obj import TridentVertex, TridentEdgeConnection
from toll_booth.alg_obj.graph.ogm.pages import PaginationToken


class Ogm:
    def __init__(self, **kwargs):
        self._trident_driver = kwargs.get('trident_driver', TridentDriver())
        self._index_driver = kwargs.get('index_driver', IndexDriver())

    def add_data(self, index_commands, graph_commands):
        self._set_indexes(index_commands)
        self._graph_objects(graph_commands)

    def _set_indexes(self, index_commands):
        with self._index_driver as pipeline:
            for index_command in index_commands:
                pipeline.execute_command(*index_command)

    def _graph_objects(self, graph_commands):
        with self._trident_driver as trident:
            for graph_command in graph_commands:
                trident.execute(graph_command)


class OgmReader:
    def __init__(self, trident_driver=None, index_driver=None):
        if not trident_driver:
            trident_driver = TridentDriver()
        if not index_driver:
            index_driver = IndexDriver()
        self._trident_driver = trident_driver
        self._index_driver = index_driver

    def execute(self, query):
        results = self._trident_driver.execute(query, True)
        return results

    def find_object(self, is_edge=False, **kwargs):
        if is_edge:
            return
        return self.get_vertex(**kwargs)

    def get_vertex(self, internal_id, **kwargs):
        vertex_label, vertex_properties = self.get_vertex_properties(internal_id, **kwargs)
        return TridentVertex(internal_id, vertex_label, vertex_properties)

    def get_vertex_properties(self, internal_id, **kwargs):
        property_names = kwargs.get('property_names', [])
        filter_string = ','.join('"{0}"'.format(w) for w in property_names)
        query = f'g.V("{internal_id}").project("vertex_properties", "vertex_label")' \
                f'.by(propertyMap([{filter_string}]))' \
                f'.by(label())'
        results = self._trident_driver.execute(query, True)
        if not results:
            return []
        return results[0]['vertex_label'], [y[0] for x, y in results[0]['vertex_properties'].items()]

    def get_edge_connection(self, internal_id, **kwargs):
        token = kwargs.get('pagination_token', PaginationToken.from_json(kwargs))
        edges, more = self.get_connected_edges(internal_id, token)
        token.increment()
        return TridentEdgeConnection(edges, token, more)

    def get_connected_edges(self, internal_id, pagination_token, **kwargs):
        edge_labels = kwargs.get('edge_labels', [])
        inclusive_start = pagination_token.inclusive_start
        exclusive_end = pagination_token.exclusive_end
        edge_label_string = ', '.join(edge_labels)
        query = f'g.V("{internal_id}")' \
                f'.outE({edge_label_string})' \
                f'.range({inclusive_start}, {exclusive_end+1})'
        edges = self._trident_driver.execute(query, True)
        returned_edges = edges[0:(exclusive_end-inclusive_start)]
        more = len(edges) > len(returned_edges)
        return self.sort_edges(internal_id, returned_edges), more

    def get_neighbors(self, internal_id, out=False, is_edge=False, get_edges=False):
        neighbors = self.calculate_neighbors(out, get_edges)
        object_letter = 'V'
        if is_edge:
            object_letter = 'E'
        query = f"g.{object_letter}('{internal_id}').{neighbors}"
        results = self._trident_driver.execute(query, True)
        if not results:
            return []
        return [x.to_gql for x in results]

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
