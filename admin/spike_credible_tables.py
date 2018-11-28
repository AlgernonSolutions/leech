import boto3
from botocore.exceptions import ClientError

from toll_booth.alg_obj.forge.extractors.credible_ws.credible_ws import CredibleReport
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


def spike_tables():
    table = boto3.resource('dynamodb').Table('VdGraphObjects')
    sql = '''
        SELECT
            Col.Table_Name, 
            Col.Column_Name            
        FROM 
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab, 
            INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col 
        WHERE 
            Col.Constraint_Name = Tab.Constraint_Name
            AND Col.Table_Name = Tab.Table_Name
            AND Constraint_Type = 'PRIMARY KEY'       
    '''
    credible_report = CredibleReport.from_sql('MBI', sql)
    with table.batch_writer() as writer:
        for table_name, entry in credible_report.items():
            try:
                column_names = [x['Column_Name'] for x in entry]
            except TypeError:
                column_names = [entry['Column_Name']]
            pairs = {
                'table_name': table_name
            }
            identifier_stem = IdentifierStem('vertex', 'CredibleTable', pairs)
            new_item = {
                'sid_value': identifier_stem.for_dynamo,
                'identifier_stem': str(identifier_stem),
                'table_name': table_name,
                'column_names': column_names
            }
            try:
                writer.put_item(Item=new_item)
            except ClientError as e:
                print(e)


if __name__ == '__main__':
    spike_tables()