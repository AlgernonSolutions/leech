import pytest

from toll_booth.alg_obj.aws.snakes.stored_statics import StaticImage


class TestStoredStatics:
    @pytest.mark.stored_image
    def test_stored_image(self):
        stored_image_asset = StaticImage.for_algernon_logo_large()
        stored_image = stored_image_asset.stored_asset
        print()
