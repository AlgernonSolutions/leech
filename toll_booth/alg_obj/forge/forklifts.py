import logging

from toll_booth.alg_obj.forge.comms.orders import ProcessObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.graph.ogm.generator import VertexCommandGenerator, EdgeCommandGenerator
from toll_booth.alg_obj.graph.ogm.ogm import Ogm


class ReachTruck:
    def __init__(self, metal_order, **kwargs):
        self._load_order = metal_order
        self._process_queue = kwargs.get('process_queue', ForgeQueue.get_for_process_queue(**kwargs))
        self._ogm = kwargs.get('ogm', Ogm(**kwargs))

    def load(self):
        graph_commands, index_commands = self._generate_commands()
        logging.info(
            f'generated commands for loading, graph_commands: {graph_commands}, index_commands: {index_commands}')
        self._ogm.add_data(index_commands, graph_commands)
        self._process_queue.add_order(ProcessObjectOrder(self._load_order.vertex))
        self._process_queue.push_orders()

    def _generate_commands(self):
        graph_commands = []
        index_commands = []
        graph_vertex_command, index_vertex_commands = self._generate_vertex_commands()
        graph_commands.append(graph_vertex_command)
        index_commands.extend(index_vertex_commands)
        if not self._load_order.edge:
            return graph_commands, index_commands
        graph_edge_command, index_edge_commands = self._generate_edge_commands()
        graph_commands.append(graph_edge_command)
        index_commands.extend(index_edge_commands)
        return graph_commands, index_commands

    def _generate_vertex_commands(self):
        vertex = self._load_order.vertex
        vertex_command_generator = VertexCommandGenerator.get_for_obj_type(vertex.object_type)
        graph_command = vertex_command_generator.create_vertex_command(vertex)
        index_commands = vertex_command_generator.create_index_graph_object_commands(vertex)
        return graph_command, index_commands

    def _generate_edge_commands(self):
        edge = self._load_order.edge
        if not edge:
            return None, None
        edge_command_generator = EdgeCommandGenerator.get_for_obj_type(edge.edge_label)
        graph_command = edge_command_generator.create_edge_command(edge)
        index_commands = edge_command_generator.create_index_graph_object_commands(edge)
        return graph_command, index_commands
