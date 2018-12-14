import boto3
from botocore.exceptions import ClientError

from toll_booth.alg_obj.forge.extractors.credible_ws.credible_ws import CredibleReport
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


def spike():
    table = boto3.resource('dynamodb').Table('VdGraphObjects')
    sql = '''
        SELECT
            clt.changelogtype_id,
            clc.category_id,
            clt.action,
            clt.has_details,
            clc.category_name,
            clt.record_type,
            clt.primarykey_name
        FROM ChangeLogType as clt
        INNER JOIN ChangeLogCategory as clc ON clt.category_id = clc.category_id
        '''
    credible_report = CredibleReport.from_sql('MBI', sql)
    with table.batch_writer() as writer:
        for id_value, entry in credible_report.items():
            pairs = {
                'category': entry['category_name'],
                'category_id': entry['category_id'],
                'action_id': entry['changelogtype_id'],
                'action': entry['action'],
                'has_details': entry['has_details']
            }
            identifier_stem = IdentifierStem('vertex', 'ChangeLogType', pairs)
            new_item = {
                'sid_value': identifier_stem.for_dynamo,
                'identifier_stem': str(identifier_stem),
                'change_category': entry.get('category_name', None),
                'change_action': entry.get('action', None),
                'has_details': entry.get('has_details', None),
                'category_id': entry.get('category_id'),
                'action_id': entry.get('changelogtype_id'),
                'id_name': entry.get('primarykey_name'),
                'id_type': entry.get('record_type')
            }
            if new_item['change_action'] == '':
                new_item['change_action'] = 'unspecified'
            if new_item['id_name'] == '':
                new_item['id_name'] = 'unspecified'
            if new_item['id_type'] == '':
                new_item['id_type'] = 'unspecified'
            try:
                writer.put_item(Item=new_item)
            except ClientError as e:
                print(e)
    print()


if __name__ == '__main__':
    spike()
