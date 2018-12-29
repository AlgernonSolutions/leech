# TODO implement this

"""
    this flow is the final step of the Leech process,
    the source object, all it's derived edges, as well as their connection points in the data space,
    are formatted into gremlin and then fired into the database
"""
from toll_booth.alg_tasks.rivers.rocks import workflow


@workflow
def load(**kwargs):
    pass