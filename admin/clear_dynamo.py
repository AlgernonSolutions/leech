import boto3

from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


def clear_dynamo_identifier_stem(identifier_stem, table_name='leech_indexes'):
    resource = boto3.resource('dynamodb').Table(table_name)
    paginator = boto3.client('dynamodb').get_paginator('query')
    iterator = paginator.paginate(
        TableName=table_name,
        KeyConditionExpression='#id=:id',
        ExpressionAttributeNames={
            '#id': 'sid_value'
        },
        ExpressionAttributeValues={
            ':id': {'S': str(identifier_stem)}
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
    identifier_stem = IdentifierStem.from_raw('#vertex#ExternalId#{\"id_source\": \"MBI\", \"id_type\": \"Employees\", \"id_name\": \"emp_id\"}#')
    clear_dynamo_identifier_stem(identifier_stem)
