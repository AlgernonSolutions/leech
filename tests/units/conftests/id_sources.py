import pytest


@pytest.fixture(params=['MBI', 'ICFS', 'PSI'])
def id_source(request):
    return request.param
