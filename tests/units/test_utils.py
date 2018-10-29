from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.utils import get_subclasses


class TestUtils:
    def test_recurse_subclasses(self):
        test = AlgObject.__subclasses__()
        subclasses = get_subclasses(AlgObject)
        print(subclasses)
