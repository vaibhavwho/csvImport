import pandas as pd
from sqlalchemy import create_engine

connection_string = 'mysql+pymysql://root:@localhost/sir'
engine = create_engine(connection_string)
global enrollment_type_change
def get_dependents_member_details(client_id, ssn_list, match_type, enrollment_type_change):
    if not isinstance(ssn_list, list):
        ssn_list = [ssn_list]

    filter_dependent = ''
    if ssn_list:
        ssn = ','.join(map(lambda x: f'"{x}"', ssn_list))
        filter_dependent = f"AND {match_type} IN ({ssn})"

    # Execute SQL query to get dependent member details
    sql = f"""
        SELECT 
            LOWER(dependent_id) as member_id, 
            LOWER({match_type}) as ssn, 
            LOWER(unique_dependent_id) as original_id, 
            LOWER(employee_id) as employee_id, 
            ts.address, 
            ts.employee_ssn, 
            ts.city, 
            ts.zip, 
            ts.latitude, 
            ts.longitude, 
            ts.state, 
            CASE WHEN dependent_gender = 1 THEN 'Male'
                 WHEN dependent_gender = 2 THEN 'Female'
                 ELSE '' END AS gender, 
            dependent_dob as dob, 
            CASE WHEN lo.option_value = 'Spouse' THEN 'SPOUSE'
                 WHEN lo.option_value = 'Child' THEN 'Child'
                 WHEN lo.option_value = 'Domestic partner' THEN 'DOMESTIC'
                 ELSE '' END AS subscriber_type,  
            CONCAT_WS(' ', dependent_first_name, dependent_last_name) as member_name 
        FROM 
            EmployeeDependents td 
            LEFT JOIN Subscribers ts ON ts.unique_member_id = td.employee_id AND ts.client_id = {client_id}
            INNER JOIN tbl_sir_lookup_options lo ON lo.option_id = td.dependent_relation_ship AND lo.option_type = 12
        WHERE 
            td.client_id = {client_id} {filter_dependent}
    """
    db_dependents_member_records = pd.read_sql_query(sql, con=engine)

    indexed_data = db_dependents_member_records.set_index('ssn').to_dict(orient='index')
    return indexed_data

def get_primary_member_details(client_id, ssn_list, match_type, enrollment_type_change):
    if not isinstance(ssn_list, list):
        ssn_list = [ssn_list]

    filter_str = ''
    if ssn_list:
        ssn = ','.join(map(lambda x: f'"{x}"', ssn_list))
        filter_str = f"AND {match_type} IN ({ssn})"

    # Execute SQL query to get primary member details
    sql = f"""
        SELECT 
            id, 
            LOWER(member_id) as member_id, 
            LOWER({match_type}) as ssn, 
            LOWER(unique_member_id) as original_id, 
            CASE WHEN gender = 1 THEN 'Male'
                 WHEN gender = 2 THEN 'Female'
                 ELSE '' END AS gender, 
            dob, 
            address, 
            employee_ssn, 
            city, 
            zip, 
            latitude, 
            longitude, 
            state, 
            'EMPLOYEE' as subscriber_type, 
            CONCAT_WS(' ', employee_first_name, employee_last_name) as member_name 
        FROM 
            Subscribers 
        WHERE 
            client_id = {client_id} {filter_str}
    """
    db_member_records = pd.read_sql_query(sql, con=engine)

    indexed_data = db_member_records.set_index('ssn').to_dict(orient='index')
    return indexed_data

def get_all_members_records(client_id, ssn_list=[], is_claims_import=False):
    if not isinstance(ssn_list, list):
        ssn_list = [ssn_list]

    is_claims_import = is_claims_import
    db_dependents_member_records = {}
    db_primary_member_records = {}
    dependent_member_ssn = []

    # Get dependent member details
    db_dependents_member_records.update(get_dependents_member_details(client_id, ssn_list, 'unique_dependent_id', enrollment_type_change))

    # Update ssn list
    employee_list = [record['employee_id'] for record in db_dependents_member_records.values()]
    ssn_list = list(set(ssn_list) - set(dependent_member_ssn))

    if ssn_list:
        ssn_list += employee_list
        db_primary_member_records.update(get_primary_member_details(client_id, ssn_list, 'unique_member_id', enrollment_type_change))

    if is_claims_import:
        db_primary_member_records.update(get_dependents_member_details(client_id, ssn_list, 'dependent_id', enrollment_type_change))
        employee_list = [record['employee_id'] for record in db_dependents_member_records.values()]
        ssn_list = list(set(ssn_list) - set(dependent_member_ssn))

        if ssn_list:
            ssn_list += employee_list
            db_primary_member_records.update(get_primary_member_details(client_id, ssn_list, 'member_id', enrollment_type_change))

    return [db_dependents_member_records, db_primary_member_records]
