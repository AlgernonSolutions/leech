import pytest

from toll_booth.alg_tasks.starter_motor import start_flow


class TestStarter:
    @pytest.mark.starter
    def test_starter(self):
        event = {
            'domain_name': 'TheLeech',
            'flow_id': '1',
            'flow_name': 'command_fungi',
            'input_string': '{"some_json": "delicious:}'
        }
        results = start_flow(event, {})
        print()
