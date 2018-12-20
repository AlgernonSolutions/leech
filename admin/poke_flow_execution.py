import boto3


def poke(task_token):
    client = boto3.client('swf')
    client.respond_decision_task_completed(**{
        'taskToken': task_token,
        'decisions': [
            {
                'decisionTaskStartedEventAttributes': {
                    'identity': 'string',
                    'scheduledEventId': 123
                }
            }
        ]
    })