# TODO implement this
"""
    this is the master workflow module,
    it contains the highest level orchestration logic needed to operate the Leech
"""
from toll_booth.alg_tasks.rivers.rocks import workflow


@workflow
def leech():
    """
        the main workflow for the leech,
        assumes that the objects in the remote source can be accessed directly by unique key
    :return:
    """
    pass


@workflow
def fungal_leech():
    """
        the incredibly weird, and hopefully soon to be retired leech,
        this is a speciality flow designed to work with FIPs (full integrated provider) in the DCDBH ICAMS system,
        it does demonstrate the concept of vertex driven growth, which may have utility elsewhere, but we hope not
    :return:
    """
    pass
