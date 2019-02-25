import logging

import boto3

from admin.set_logging import set_logging
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


def clear_dynamo_table(table_name='leech_indexes'):
    resource = boto3.resource('dynamodb').Table(table_name)
    paginator = boto3.client('dynamodb').get_paginator('scan')
    iterator = paginator.paginate(TableName=table_name)
    with resource.batch_writer() as writer:
        for page in iterator:
            for item in page['Items']:
                writer.delete_item(Key={
                    'sid_value': item['sid_value']['S'],
                    'identifier_stem': item['identifier_stem']['S']
                })


def clear_dynamo_identifier_stem(identifier_stem, table_name='leech_indexes'):
    resource = boto3.resource('dynamodb').Table(table_name)
    paginator = boto3.client('dynamodb').get_paginator('query')
    iterator = paginator.paginate(
        TableName=table_name,
        KeyConditionExpression='#id=:id',
        ExpressionAttributeNames={
            '#id': 'identifier_stem'
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
    set_logging()
    clear_dynamo_table()
    icfs_emp_identifier_string = '#vertex#ExternalId#{\"id_source\": \"ICFS\", \"id_type\": \"Employees\", \"id_name\": \"emp_id\"}#'
    mbi_emp_identifier_string = '#vertex#ExternalId#{\"id_source\": \"MBI\", \"id_type\": \"Employees\", \"id_name\": \"emp_id\"}#'
    icfs_edge_identifier_string = '#edge#_fip_link_#{\"id_source\": \"ICFS\", \"id_type\": \"Employees\", \"id_name\": \"emp_id\"}#'
    mbi_edge_identifier_string = '#edge#_fip_link_#{\"id_source\": \"MBI\", \"id_type\": \"Employees\", \"id_name\": \"emp_id\"}#'
    blank_edge_identifier_string = '#edge#_by_emp_id_#{}#'
    changed_edge = '#edge#_changed_#{}#'
    icfs_strings = [icfs_emp_identifier_string, icfs_edge_identifier_string]
    mbi_string = [mbi_emp_identifier_string, mbi_edge_identifier_string]
    all_strings = [icfs_edge_identifier_string, icfs_emp_identifier_string, mbi_edge_identifier_string, mbi_emp_identifier_string, blank_edge_identifier_string, changed_edge]
    blank_strings = [blank_edge_identifier_string]
    for entry in all_strings:
        target_identifier_stem = IdentifierStem.from_raw(entry)
        logging.info(f'started a clear dynamo identifier stem operation: {target_identifier_stem}')
        clear_dynamo_identifier_stem(target_identifier_stem)
        logging.info(f'completed clearing identifier stem: {target_identifier_stem}')
