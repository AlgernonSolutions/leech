from toll_booth.alg_obj.forge.extractors.credible_ws.credible_ws import CredibleReport


def find_table(domain_name, table_name=None):
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
    if table_name:
        sql += f" AND Col.Table_Name LIKE '%{table_name}%'"
    credible_report = CredibleReport.from_sql(domain_name, sql)
    return credible_report.data


if __name__ == '__main__':
    find_table('MBI', 'noti')
