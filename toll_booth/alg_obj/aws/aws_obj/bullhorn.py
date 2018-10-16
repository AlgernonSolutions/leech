import boto3

sns = boto3.resource('sns')


class EventManager:
    @classmethod
    def unsubscribe_recipient(cls, subscriber, **kwargs):
        subscriber = sns.Topic(subscriber.identifier)
        subscriber.delete()

    @classmethod
    def subscribe_recipient(cls, event_type, subscriber):
        end_point = subscriber.end_point
        protocol = subscriber.protocol
        topic = sns.Topic(event_type.identifier)
        subscription = topic.subscribe(
            Protocol=protocol,
            Endpoint=end_point,
            ReturnSubscriptionArn=True
        )
        return subscription
