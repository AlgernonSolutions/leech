from toll_booth.alg_obj.aws.aws_obj.lockbox import IndexDriver
from toll_booth.alg_obj.aws.trident.graph_driver import TridentDriver


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

    def find_object(self, internal_id, is_edge=False):
        object_letter = 'V'
        if is_edge:
            object_letter = 'E'
        query = f"g.{object_letter}('{internal_id}')"
        results = self._trident_driver.execute(query, True)
        if not results:
            return
        return results[0].to_gql

    def get_edges(self, internal_id, out=False):
        edge_direction = 'inE()'
        if out:
            edge_direction = 'outE()'
        query = f"g.V('{internal_id}').{edge_direction}"
        results = self._trident_driver.execute(query, True)
        if not results:
            return []
        return [x.to_gql for x in results]
