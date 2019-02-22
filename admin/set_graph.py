import os

from toll_booth.alg_obj.graph import InternalId
from toll_booth.alg_obj.graph.ogm.ogm import Ogm
from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex, IdentifierStem


def add_data_source_vertex(id_source, **kwargs):
    internal_id = InternalId(''.join(['IdSource', id_source])).id_value
    identifier_stem = IdentifierStem('vertex', 'IdSource')
    potential_vertex = PotentialVertex('IdSource', internal_id, {}, identifier_stem, id_source, 'id_source')
    ogm = Ogm(**kwargs)
    results = ogm.graph_objects(vertexes=[potential_vertex])
    return results


if __name__ == '__main__':
    id_sources = ['ICFS', 'MBI', 'PSI']
    os.environ['GRAPH_DB_ENDPOINT'] = 'algernon.cluster-cnv3iqiknsnm.us-east-1.neptune.amazonaws.com'
    os.environ['GRAPH_DB_READER_ENDPOINT'] = 'algernon.cluster-ro-cnv3iqiknsnm.us-east'
    for entry in id_sources:
        add_data_source_vertex(entry)
