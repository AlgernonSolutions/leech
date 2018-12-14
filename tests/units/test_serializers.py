import datetime
import json

import pytest
import pytz

from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder


@pytest.mark.serializers
class TestAlgSerializers:
    def test_serialize_datetime_utc(self):
        test_date_time = datetime.datetime.now(pytz.UTC)
        test_object = {
            'test_date_time': test_date_time
        }
        serialized = json.dumps(test_object, cls=AlgEncoder)
        back_again = json.loads(serialized, cls=AlgDecoder)
        assert back_again == test_object

    def test_serialize_local_datetime(self):
        eastern = pytz.timezone('US/Eastern')
        test_date_time = datetime.datetime.now()
        local_test_date_time = eastern.localize(test_date_time)
        test_object = {
            'test_date_time': local_test_date_time
        }
        serialized = json.dumps(test_object, cls=AlgEncoder)
        back_again = json.loads(serialized, cls=AlgDecoder)
        assert back_again == test_object
