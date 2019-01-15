import pytest

from tests.units.test_data import patches
from tests.units.test_data.data_setup.boto import intercept


@pytest.fixture
def fungus_environment():
    boto_patch = patches.get_boto_patch()
    mock_boto = boto_patch.start()
    mock_boto.side_effect = intercept
    yield mock_boto
    boto_patch.stop()
