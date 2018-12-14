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
    clear_propagation_id('a432c6e023e848a88207f7ab8791e538')
