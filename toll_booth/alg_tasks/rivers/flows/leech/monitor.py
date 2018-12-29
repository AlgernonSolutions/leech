# TODO implement this

"""
    This flow is the first step in the Leech process,
    it monitors an assigned remote data source on an assigned identifier_stem, for new unique identifiers,
    if new identifiers are detected, they are forwarded onto the extractor
"""
from toll_booth.alg_tasks.rivers.rocks import workflow


@workflow
def monitor(**kwargs):
    pass
