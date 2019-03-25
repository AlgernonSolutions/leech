from toll_booth.alg_obj import AlgObject


class FungalStem(AlgObject):
    def __init__(self, id_source, driving_id_type, driving_id_value, change_category):
        self._id_source = id_source
        self._driving_id_type = driving_id_type
        self._driving_id_value = driving_id_value
        self._change_category = change_category

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['id_source'], json_dict['driving_id_type'],
            json_dict['driving_id_value'], json_dict['change_category']
        )

    @classmethod
    def from_identifier_stem(cls, identifier_stem, driving_id_value, change_category):
        return cls(identifier_stem.get('id_source'), identifier_stem.get('id_type'), driving_id_value, change_category)

    def __str__(self):
        return f'#{self._id_source}#{self._driving_id_type}#{self._driving_id_value}#{self._change_category}'