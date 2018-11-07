import pytest

from toll_booth.alg_tasks.toll_booth import work

test_event = {'task_name': 'load', 'task_args': {'keys': {'sid_value': {'S': '3178'}, 'identifier_stem': {'S': '#vertex#ExternalId#{"id_source": "MBI", "id_type": "Employees", "id_name": "emp_id"}#'}}}}
other_event = {'task_name': 'load', 'task_args': {'keys': {'sid_value': {'S': '3134'}, 'identifier_stem': {'S': '#vertex#ExternalId#{"id_source": "MBI", "id_type": "Employees", "id_name": "emp_id"}#'}}}}


@pytest.mark.test_load
@pytest.mark.usefixtures('silence_x_ray')
class TestLoad:
    @pytest.mark.parametrize(
        'event', [
            test_event,
            other_event
        ]
    )
    def test_load(self, event, mock_context, mock_neptune):
        results = work(event, mock_context)
        mock_neptune.assert_called_once()
        print()
