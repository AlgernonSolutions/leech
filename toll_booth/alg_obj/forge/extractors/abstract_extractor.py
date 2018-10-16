from abc import ABC


class AbstractExtractor(ABC):

    def get_current_remote_max_id(self):
        pass

    def extract(self):
        pass
