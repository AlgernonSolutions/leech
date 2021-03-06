import json
import re

import pytest

from toll_booth.alg_tasks.log_process import handler, transform_log_event

test_event = {
    'invocationId': '468b3ce2-8134-40eb-9212-6c11a5ac436b',
    'deliveryStreamArn': 'arn:aws:firehose:us-east-1:803040539655:deliverystream/lambda',
    'region': 'us-east-1',
    'records': [
        {
            'recordId': '49589735566064537801098039047497717495567350915787653122000000',
            'approximateArrivalTimestamp': 1541087166013,
            'data': 'H4sIAAAAAAAAADWOXQuCMBiF/8rYtYSC9uFdiHpjCSl0ERJL39xIN9lmEuJ/b6ZdPpzDOc+IW1CK1JB/OsA+DtJzfkmT+ynMsmMcYguLgYOck0b01UB0SRNRKxM0oo6l6DuTLZRpCaRdUPUPVUrWaSZ4xBoNUmH/Vvx64Ru4nnHErFrqmhkNTVoz5niuY+93ztY7uLb115sFrgla9dCq56OAQvlivEYUSKMpEk9UmSXGyfyMIiaBCgUbPBXTFwk/BdTsAAAA'}, {'recordId': '49589735566064537801098039048249669355367652873795010562000000', 'approximateArrivalTimestamp': 1541087204666, 'data': 'H4sIAAAAAAAAADWQS2vDMBCE/4oRPcZYa+3q4Zuhbii0l9j00oYiW3Jq8CO1nYYS8t+rvm7L7MfMzl7Y4JfFHnz1efQsY7d5lb8+FmWZbwu2YdN59HOQNRccOQkjiYLcT4ftPJ2OYZPY85L0dqidTXrvm7d4mMZuneZfrFxnb4fApRx0ApBwSJ5vHvKqKKu9SzmiwtZZaBBba8kAkHGgvAfSIlgsp3pp5u64dtN41/WrnxeWPbO2mz3b/yQUH35cv8UL61wIEiikEkZwiQAS0YQZjQx+yLVClJqDUmg0ak5ap4YAuCQZwtYuPGO1Q+gFhBDwlAtDtPl/UrAvq3xXRTv/fgrovcsiIsC6UT52zrsYwOvY1FzFVFNrOIFosImewt2hQRb9VX8Z2XV//QIjD+nxfQEAAA=='}, {'recordId': '49589735566064537801098039049299016966793152028232122370000000', 'approximateArrivalTimestamp': 1541087219665, 'data': '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'}]}


class TestLogProcess:
    @pytest.mark.parametrize(
        'event', [
            test_event
        ]
    )
    @pytest.mark.log_process
    def test_log_process(self, event):
        results = handler(event, [])
        print(results)

    @pytest.mark.parametrize(
        'message', [
            '[INFO] ||function_name: test|function_arn: arn:aws:lambda:us-east-1:803040539655:function:test|request_id: e82ee959-deb7-11e8-bbf4-37a181b26d1e|| 2018-11-02 15:56:49,302 test the logging my son'
        ]
    )
    @pytest.mark.log_transform
    def test_log_transform_python_log(self, message):
        log_event = {'timestamp': 1541087166013, 'message': message}
        results = transform_log_event(log_event)
        assert isinstance(results, str)
        log_object = json.loads(results)
        assert 'log_level' in log_object
        assert log_object['log_level'] in ['INFO', 'DEBUG', 'WARNING', 'ERROR']
        assert 'log_timestamp' in log_object
        assert re.compile('(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3})').match(log_object['log_timestamp'])
        assert 'function_arn' in log_object
        assert re.compile('[\w,:-]+').match(log_object['function_arn'])
        assert 'request_id' in log_object
        assert re.compile('[\w\d-]+').match(log_object['request_id'])
        assert 'log_message' in log_object
        assert 'timestamp' in log_object
        assert isinstance(log_object['timestamp'], int)
