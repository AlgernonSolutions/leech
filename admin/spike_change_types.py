import boto3

from toll_booth.alg_obj.forge.extractors.credible_ws.credible_ws import CredibleReport
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


def spike():
    table = boto3.resource('dynamodb').Table('VdGraphObjects')
    sql = '''
        SELECT
            clt.changelogtype_id,
            clt.action,
            clt.has_details,
            clc.category_name
        FROM ChangeLogType as clt
        INNER JOIN ChangeLogCategory as clc ON clt.category_id = clc.category_id'''
    credible_report = CredibleReport.from_sql('MBI', sql)
    with table.batch_writer() as writer:
        for id_value, entry in credible_report.items():
            pairs = {
                'category': entry['category_name'],
                'action': entry['action'],
                'has_details': entry['has_details']
            }
            identifier_stem = IdentifierStem('vertex', 'ChangeLogType', pairs)
            new_item = {
                'sid_value': identifier_stem.for_dynamo,
                'identifier_stem': str(identifier_stem)
            }
            writer.put_item(Item=new_item)
    print()


if __name__ == '__main__':
    spike()