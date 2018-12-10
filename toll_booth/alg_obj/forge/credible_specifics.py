from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


class ChangeType:
    def __init__(self, category_id, category, action_id, action, has_details):
        self._category_id = category_id
        self._category = category
        self._action_id = action_id
        self._action = action
        self._has_details = has_details

    @classmethod
    def get_from_change_identifier(cls, change_identifier):
        change_identifier = IdentifierStem.from_raw(change_identifier)
        return cls(**change_identifier.paired_identifiers)

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
    def has_details(self):
        return self._has_details

    def __str__(self):
        return self._action


class ChangeTypeCategory:
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


class ChangeTypes:
    def __init__(self, change_types=None):
        if not change_types:
            change_types = {}
        self._change_types = change_types

    @classmethod
    def get(cls, **kwargs):
        from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
        change_types = {}
        leech_driver = kwargs.get('leech_driver', LeechDriver(**kwargs))
        change_type_data = leech_driver.get_changelog_types()
        for category_name, entries in change_type_data.items():
            for entry in entries:
                change_type = ChangeType(**entry.paired_identifiers)
                change_types[change_type.action_id] = change_type
        return cls(change_types)

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

    def values(self):
        return self._change_types.values()

    def get_action_id(self, action):
        for change_type in self.actions:
            if action == change_type.action:
                return change_type.action_id
        raise AttributeError

