import pytest
from mock import patch

from tests.units.test_data import patches
from toll_booth.alg_tasks.remote_tasks.explode import explode


@pytest.mark.exploded
class TestExploded:
    @pytest.mark.explode
    def test_empty_explode(self, dynamo_stream_event):
        with patch(patches.boto_patch):
            explode_event = {
                'task_name': 'explode',
                'task_args': {
                    'records': dynamo_stream_event['Records']
                }
            }
            explode(explode_event['task_args'], context={})
