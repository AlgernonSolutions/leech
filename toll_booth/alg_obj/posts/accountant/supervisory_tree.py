class SupervisionBranch:
    def __init__(self, branch_id, supervisor_ids=None, supervised_ids=None):
        if not supervised_ids:
            supervised_ids = []
        if not supervisor_ids:
            supervisor_ids = []
        self._branch_id = branch_id
        self._supervisor_ids = supervisor_ids
        self._supervised_ids = supervised_ids

    @property
    def branch_id(self):
        return self._branch_id

    @property
    def supervisor_ids(self):
        return self._supervisor_ids

    @property
    def supervised_ids(self):
        return self._supervised_ids

    def add_supervised_id(self, supervised_id):
        if supervised_id not in self._supervised_ids:
            self._supervised_ids.append(supervised_id)

    def add_supervisor_id(self, supervisor_id):
        if supervisor_id not in self._supervisor_ids:
            self._supervisor_ids.append(supervisor_id)


class SupervisorTree:
    def __init__(self):
        self._branches = {}

    def add_branch(self, branch):
        branch_id = branch.branch_id
        supervisor_ids = branch.supervisor_ids
        supervised_ids = branch.supervised_ids
        self._branches[branch_id] = branch
        for supervisor_id in supervisor_ids:
            if supervisor_id not in self._branches:
                self._branches[supervisor_id] = SupervisionBranch(supervisor_id)
            self._branches[supervisor_id].add_supervised_id(branch_id)
        for supervised_id in supervised_ids:
            if supervised_id not in self._branches:
                self._branches[supervised_id] = SupervisionBranch(supervised_id)
            self._branches[supervised_id].add_supervisor_id(branch_id)

    def get_parents(self, branch_id):
        parents = []
        branch = self._branches[branch_id]
        for supervisor_id in branch.supervisor_ids:
            parents.append(supervisor_id)
            parents.extend(self.get_parents(supervisor_id))
        return parents

    def get_children(self, branch_id):
        children = []
        branch = self._branches[branch_id]
        for supervised_id in branch.supervised_ids:
            children.append(supervised_id)
            children.extend(self.get_children(supervised_id))
        return children

    def __contains__(self, item):
        return item in self._branches

    def __getitem__(self, item):
        return self._branches[item]
