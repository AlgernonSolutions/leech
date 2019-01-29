import os


class Inquisitor:
    def __init__(self, table_name=None, graph_db_reader_address=None):
        if not table_name:
            table_name = os.getenv('TABLE_NAME', 'Leech')
        if not graph_db_reader_address:
            graph_db_reader_address = os.getenv('GRAPH_DB_READER_ENDPOINT')
        self._table_name = table_name
        self._graph_db_reader_address = graph_db_reader_address
