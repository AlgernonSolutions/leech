from toll_booth.alg_obj.aws.sapper.dynamo_driver import LeechDriver


class MonitorSnake:
    def __init__(self, identifier_stem):
        self._identifier_stem = identifier_stem
        self._dynamo_driver = LeechDriver(table_name='VdGraphObjects')

    def monitor(self):
        extraction_names = self._dynamo_driver.get_extractor_function_names(self._identifier_stem)
        print()