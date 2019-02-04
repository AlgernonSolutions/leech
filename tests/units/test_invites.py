import pytest

from toll_booth.alg_obj.aws.snakes.invites import ObjectDownloadLink


@pytest.mark.invites
class TestInvites:
    def test_invites(self):
        bucket_name = 'algernonsolutions-leech'
        file_name = 'PSI/samples/PSI_demo_reports.xlsx'
        invite = ObjectDownloadLink(bucket_name, file_name)
        invite_url = str(invite)
        print()
