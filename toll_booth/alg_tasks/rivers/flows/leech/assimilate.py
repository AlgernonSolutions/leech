# TODO implement this

"""
    this flow allows the data extracted from the remote source to be joined into the existing data space, per the schema
    the process of assimilation requires a direct query of the graph database, so this flow should be constrained to match
"""
from toll_booth.alg_tasks.rivers.rocks import workflow


@workflow
def load(**kwargs):
    pass