from toll_booth.alg_obj.aws.matryoshkas.clerks import ClerkSwarm


class TestClerkSwarm:
    def test_clerk_swarm(self):
        entries = [{'identifier_stem': 'test_item', 'sid_value': str(x), 'is_test': True} for x in range(0, 25)]
        clerks = ClerkSwarm('VdGraphObjects', pending_entries=entries)
        results = clerks.send()
        print()
