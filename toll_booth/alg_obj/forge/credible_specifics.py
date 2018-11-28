from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver


class ChangeType:
    def __init__(self, category_id, category, action_id, action, has_details):
        self._category_id = category_id
        self._category = category
        self._action_id = action_id
        self._action = action
        self._has_details = has_details

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
        change_types = {}
        leech_driver = kwargs.get('leech_driver', LeechDriver(**kwargs))
        change_type_data = leech_driver.get_changelog_types()
        for entry in change_type_data:
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
