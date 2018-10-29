import importlib
import pkgutil

from toll_booth.alg_obj import AlgObject

for (module_loader, name, ispkg) in pkgutil.iter_modules(['leech']):
    importlib.import_module('.' + name, __package__)

from toll_booth.alg_obj.utils import get_subclasses


class TestUtils:
    def test_recurse_subclasses(self):
        test = AlgObject.__subclasses__()
        subclasses = get_subclasses(AlgObject)
        print(subclasses)
