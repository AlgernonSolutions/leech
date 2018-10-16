class MockRedis:
    @classmethod
    def get_for_monitor(cls, context):
        return [
            [('hey_now'.encode(), str(context.local_maximum_value))],
            [('whats that sound'.encode(), str(context.local_maximum_value - 10))]
        ]
