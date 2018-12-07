import boto3


def clear_propagation_id(propagation_id, table_name='VdGraphObjects'):
    resource = boto3.resource('dynamodb').Table(table_name)
    paginator = boto3.client('dynamodb').get_paginator('query')
    iterator = paginator.paginate(
        TableName=table_name,
        KeyConditionExpression='#sid=:pid',
        ExpressionAttributeNames={
            '#sid': 'sid_value'
        },
        ExpressionAttributeValues={
            ':pid': {'S': propagation_id}
        }
    )
    with resource.batch_writer() as writer:
        for page in iterator:
            for item in page['Items']:
                writer.delete_item(Key={
                    'sid_value': item['sid_value']['S'],
                    'identifier_stem': item['identifier_stem']['S']
                })


if __name__ == '__main__':
    clear_propagation_id('fc89d151e61f4c5bb943d41eef0fbaf0')
