import os

from toll_booth.alg_obj.graph.ogm.ogm import Ogm


def clear_graph(source_query, **kwargs):
    ogm = Ogm(**kwargs)
    results = ogm.execute(f'{source_query}.drop()')
    return results


if __name__ == '__main__':
    os.environ['GRAPH_DB_ENDPOINT'] = 'algernon.cluster-cnv3iqiknsnm.us-east-1.neptune.amazonaws.com'
    os.environ['GRAPH_DB_READER_ENDPOINT'] = 'algernon.cluster-ro-cnv3iqiknsnm.us-east'
    target_query = 'g.V().has("id_source", "ICFS").bothE().hasLabel("_changed_")'
    results = clear_graph(target_query)
    print(results)
