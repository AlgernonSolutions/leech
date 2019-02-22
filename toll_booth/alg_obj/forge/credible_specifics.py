from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


class ChangeType(AlgObject):
    def __init__(self, category_id, category, action_id, action, id_type, id_name, has_details, **kwargs):
        is_static = kwargs.get('is_static', False)
        entity_type = kwargs.get('entity_type')
        self._category_id = category_id
        self._category = category
        self._action_id = action_id
        self._action = action
        self._id_type = id_type
        self._id_name = id_name
        self._has_details = has_details
        self._is_static = is_static
        self._entity_type = entity_type

    @classmethod
    def get_from_change_identifier(cls, change_identifier):
        change_identifier = IdentifierStem.from_raw(change_identifier)
        return cls(**change_identifier.paired_identifiers)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['category_id'], json_dict['category'], json_dict['action_id'], json_dict['action'],
            json_dict['id_type'], json_dict['id_name'], json_dict['has_details'], is_static=json_dict.get('is_static')
        )

    @property
    def category_id(self):
        return self._category_id

    @property
    def category(self):
        return self._category

    @property
    def action_id(self):
        return self._action_id

    @property
    def action(self):
        return self._action

    @property
    def id_name(self):
        return self._id_name

    @property
    def id_type(self):
        return self._id_type

    @property
    def has_details(self):
        return self._has_details

    @property
    def is_static(self):
        return self._is_static

    @property
    def entity_type(self):
        return self._entity_type

    @property
    def has_client_entity(self):
        return self._entity_type == 'Clients'

    @property
    def has_emp_entity(self):
        return self._entity_type == 'Employees'

    def __str__(self):
        return self._action


class ChangeTypeCategory(AlgObject):
    def __init__(self, category_id, category, change_types=None):
        if not change_types:
            change_types = {}
        self._category_id = category_id
        self._category = category
        self._change_types = change_types

    @classmethod
    def get_from_change_identifiers(cls, change_identifiers):
        change_types = {}
        for change_identifier in change_identifiers:
            change_type = ChangeType.get_from_change_identifier(change_identifier)
            change_types[change_type.action_id] = change_type
        for change_type in change_types.values():
            return cls(change_type.category_id, change_type.category, change_types)

    @classmethod
    def get_from_change_name(cls, change_name, **kwargs):
        driver = kwargs.get('driver', None)
        if not driver:
            from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
            driver = LeechDriver(table_name=kwargs.get('table_name', 'VdGraphObjects'))
        results = driver.get_changelog_types(category=change_name)
        return cls.get_from_change_identifiers(results)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['category_id'], json_dict['category'], json_dict.get('change_types', None))

    @property
    def category_id(self):
        return self._category_id

    @property
    def category(self):
        return self._category

    @property
    def change_types(self):
        return self._change_types

    def add_change_type(self, change_type):
        if change_type.category_id != self._category_id:
            raise RuntimeError(
                'tried to add change_type with category_id: %s '
                'to a ChangeTypeCategory it does not belong to, '
                'parent category_id: %s' % (change_type.category_id, self._category_id))
        self._change_types[change_type.action_id] = change_type

    def get_action_id(self, action):
        for change_type in self._change_types.values():
            if change_type.action == action:
                return change_type.action_id
        raise AttributeError

    def action_has_details(self, action_id):
        return self._change_types[action_id].has_details

    def __str__(self):
        return self._category

    def __getitem__(self, item):
        return self._change_types[item]

    def values(self):
        return self._change_types.values()

    def items(self):
        return self._change_types.items()


class ChangeTypes(AlgObject):
    def __init__(self, change_types=None):
        if not change_types:
            change_types = {}
        self._change_types = change_types

    @classmethod
    def retrieve(cls, **kwargs):
        from toll_booth.alg_obj.aws.snakes.schema_snek import SchemaSnek

        snek = SchemaSnek(**kwargs)
        change_types = {}
        change_type_data = snek.get_schema('change_types.json')
        for entry in change_type_data:
            entry['category'] = entry['change_category']
            entry['action'] = entry['change_action']
            change_type = ChangeType(**entry)
            change_types[change_type.action_id] = change_type
        return cls(change_types)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['change_types'])

    @property
    def categories(self):
        categories = {}
        for action_id, change_type in self._change_types.items():
            category_id = change_type.category_id
            if category_id not in categories:
                categories[category_id] = ChangeTypeCategory(category_id, change_type.category)
            categories[category_id].add_change_type(change_type)
        return categories

    @property
    def grouped_categories(self):
        categories = {}
        for action_id, change_type in self._change_types.items():
            category_id = change_type.category_id
            if category_id not in categories:
                categories[category_id] = []
            categories[category_id].append(change_type)
        return categories

    @property
    def grouped_actions(self):
        return self._change_types

    @property
    def actions(self):
        return list(self._change_types.values())

    def __getitem__(self, item):
        return self._change_types.get(item)

    def values(self):
        return self._change_types.values()

    def get_action_id(self, action):
        for change_type in self.actions:
            if action == change_type.action:
                return change_type.action_id
        raise AttributeError

    def get_category_id_from_action_id(self, action_id):
        change_action = self._change_types[action_id]
        return change_action.category_id

    def get_category_by_name(self, category_name):
        for category_id, category in self.categories.items():
            if category.category == category_name:
                return self.categories[category_id]
        raise AttributeError
