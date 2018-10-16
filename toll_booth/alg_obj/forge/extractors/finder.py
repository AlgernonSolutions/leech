from toll_booth.alg_obj.forge.extractors import credible_ws


class ExtractorFinder:
    extractor_modules = [credible_ws]

    @classmethod
    def find_by_name(cls, extractor_name):
        for module in cls.extractor_modules:
            try:
                task_module = getattr(module, extractor_name)
                return task_module
            except AttributeError:
                continue
        raise RuntimeError
