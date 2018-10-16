class MockObjectIdentifier:
    @classmethod
    def get(cls, context):
        trial_data = context.active_params
        return {
            'id_source': trial_data['source'],
            'object_type': trial_data['object_type'],
            'id_name': trial_data['id_name'],
            'id_type': trial_data['id_type']
        }

    @classmethod
    def get_extraction_source(cls, context):
        trial_data = context.active_params
        return trial_data['method']
