from datetime import datetime

import dateutil
import pytest

from toll_booth.alg_obj.aws.snakes.stored_statics import StaticImage, StaticCsv, StaticJson


class TestStoredStatics:
    @pytest.mark.stored_csv
    def test_stored_csv(self):
        stored_csv = StaticJson.for_team_data('ICFS')
        stored_image = stored_csv.stored_asset
        teams = stored_csv['teams']
        print()
