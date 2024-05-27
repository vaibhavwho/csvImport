import pdb
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from constants import connection_string
from save_filter import save_filter_array

engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)
session = Session()

db_dependents_member_records = {}
db_primary_member_records = {}


def get_dependents_member_details(client_id, ssn_list, match_type, enrollment_type_change):
    global db_dependents_member_records

    if not isinstance(ssn_list, list):
        ssn_list = [ssn_list]

    filter_dependent = ''
    if ssn_list:
        ssn_query = save_filter_array(ssn_list, 'ssn_trigger', [], client_id)
        # ssn_res = pd.read_sql_query(ssn_query, engine)
        # ssn = ssn_res['filter_values'].tolist()
        # ssn = tuple(ssn)
        # formatted_ssn = ", ".join(f"'{filter_values}'" for filter_values in ssn)
        # ssn_list_tuple = tuple(ssn_list)
        # unique_patient_ids = ", ".join(f"'{unique_patient_id}'" for unique_patient_id in ssn_list_tuple)
        filter_dependent = f" AND employee_id IN ({ssn_query})" if enrollment_type_change == 'grid' else f" AND {match_type} IN ({ssn_query})"

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
            tbl_ph_employee_dependents td 
            LEFT JOIN tbl_ph_subscribers ts ON ts.unique_member_id = td.employee_id AND ts.client_id = {client_id}
            INNER JOIN tbl_sir_lookup_options lo ON lo.option_id = td.dependent_relation_ship AND lo.option_type = 12
        WHERE 
            td.client_id = {client_id} {filter_dependent}
    """
    #print(f"Executing SQL: {sql} with params: {params}")
    data_dependent = pd.read_sql_query(sql, con=engine)

    # Correctly index the data by ssn and update the global dictionary
    indexed_data = data_dependent.set_index('ssn').to_dict(orient='index')
    db_dependents_member_records.update(indexed_data)


def get_primary_member_details(client_id, ssn_list, match_type, enrollment_type_change):
    global db_primary_member_records

    if not isinstance(ssn_list, list):
        ssn_list = [ssn_list]

    filter_str = ''
    if ssn_list:
        ssn_query = save_filter_array(ssn_list, 'ssn_trigger', [], client_id)
        filter_str = f"AND {match_type} IN ({ssn_query})"

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
            tbl_ph_subscribers 
        WHERE 
            client_id = {client_id} {filter_str}
    """

    # print(f"Executing SQL: {sql}")
    data_primary = pd.read_sql_query(sql, con=engine)
    # Correctly index the data by ssn and update the global dictionary
    indexed_data = data_primary.set_index('ssn').to_dict(orient='index')
    db_primary_member_records.update(indexed_data)


def get_all_members_records(client_id, ssn_list, is_claims_import=False):
    global db_dependents_member_records, db_primary_member_records

    if ssn_list is None:
        ssn_list = []

    if not isinstance(ssn_list, list):
        ssn_list = [ssn_list]

    db_dependents_member_records = {}
    db_primary_member_records = {}
    enrollment_type_change = None
    # Check in dependents
    get_dependents_member_details(client_id, ssn_list, 'unique_dependent_id', enrollment_type_change)
    employee_list = list(set(record['employee_id'] for record in db_dependents_member_records.values()))
    ssn_list = list(set(ssn_list) - set(db_dependents_member_records.keys()))

    if ssn_list:
        ssn_list = ssn_list + employee_list
        get_primary_member_details(client_id, ssn_list, 'unique_member_id', enrollment_type_change)

        if is_claims_import:
            get_dependents_member_details(client_id, ssn_list, 'dependent_id', enrollment_type_change)
            employee_list = list(set(record['employee_id'] for record in db_dependents_member_records.values()))
            ssn_list = list(set(ssn_list) - set(db_dependents_member_records.keys()))

            if ssn_list:
                ssn_list = ssn_list + employee_list
                get_primary_member_details(client_id, ssn_list, 'member_id', enrollment_type_change)

    return {
        'db_dependents_member_records': db_dependents_member_records,
        'db_primary_member_records': db_primary_member_records
    }
