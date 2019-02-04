import pytest


@pytest.fixture
def branch_id():
    return 1001


@pytest.fixture
def branch_data():
    return 1001, [1000], [10002, 1003]
