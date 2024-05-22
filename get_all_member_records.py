import random
import pandas as pd
from sqlalchemy import create_engine
import time
from datetime import datetime, timedelta
from constants import connection_string
from save_filter import save_filter_array


engine = create_engine(connection_string)

# Global variables to hold records and SSNs
global enrollment_type_change
db_dependents_member_records = {}
dependent_member_ssns = []


def get_dependents_member_details(client_id, ssn_list, match_type, enrollment_type_change):
    global db_dependents_member_records

    if not isinstance(ssn_list, list):
        ssn_list = [ssn_list]

    filter_dependent = ''
    if ssn_list:
        ssn_query = save_filter_array(ssn_list, 'ssn_trigger', [], client_id)
        if enrollment_type_change == 'grid':
            filter_dependent = f"AND employee_id IN ({ssn_query})"
        else:
            filter_dependent = f"AND {match_type} IN ({ssn_query})"

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
            tbl_ph_employee_dependents td 
            LEFT JOIN tbl_ph_subscribers ts ON ts.unique_member_id = td.employee_id AND ts.client_id = {client_id}
            INNER JOIN tbl_sir_lookup_options lo ON lo.option_id = td.dependent_relation_ship AND lo.option_type = 12
        WHERE 
            td.client_id = {client_id} {filter_dependent}
    """
    data_dependent = pd.read_sql_query(sql, con=engine)

    # Use the indexing and merging function
    merged_records, merged_ssns = index_and_merge_data(data_dependent)

    return merged_records, merged_ssns


def index_and_merge_data(data_dependent):
    global db_dependents_member_records, dependent_member_ssns

    # Index the data by 'ssn'
    indexed_data = data_dependent.set_index('ssn').to_dict(orient='index')
    # Merge indexed data with existing records
    merged_records = {**db_dependents_member_records, **indexed_data}
    db_dependents_member_records.update(merged_records)
    # Extract SSNs from merged records
    indexed_ssn = list(merged_records.keys())
    # Merge SSNs with existing SSNs
    dependent_member_ssns = list(set(dependent_member_ssns + indexed_ssn))
    return merged_records, dependent_member_ssns


def get_primary_member_details(client_id, ssn_list, match_type, enrollment_type_change):
    global db_dependents_member_records

    if not isinstance(ssn_list, list):
        ssn_list = [ssn_list]

    filter_str = ''
    if ssn_list:
        ssn_query = save_filter_array(ssn_list, 'ssn_trigger', [], client_id)
        if enrollment_type_change == 'grid':
            filter_str = f"AND id IN ({ssn_query})"
        else:
            filter_str = f"AND {match_type} IN ({ssn_query})"

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
            tbl_ph_subscribers 
        WHERE 
            client_id = {client_id} {filter_str}
    """
    data_primary = pd.read_sql_query(sql, con=engine)

    merged_records, merged_ssns = index_and_merge_data(data_primary)

    return merged_records, merged_ssns


def get_all_members_records(client_id, ssn_list=[], is_claims_import=False):
    global db_dependents_member_records, dependent_member_ssns, enrollment_type_change

    if not isinstance(ssn_list, list):
        ssn_list = [ssn_list]

    enrollment_type_change = 'grid'
    db_dependents_member_records = {}
    db_primary_member_records = {}
    dependent_member_ssns = []

    # Get dependent member details
    dependents_records, dependent_ssns = get_dependents_member_details(client_id, ssn_list, 'unique_dependent_id', enrollment_type_change)
    db_dependents_member_records.update(dependents_records)

    # Update ssn list
    employee_list = [record['employee_id'] for record in db_dependents_member_records.values()]
    ssn_list = list(set(ssn_list) - set(dependent_member_ssns))

    if ssn_list:
        ssn_list += employee_list
        primary_records, primary_ssns = get_primary_member_details(client_id, ssn_list, 'unique_member_id', enrollment_type_change)
        db_primary_member_records.update(primary_records)

    if is_claims_import:
        dependents_records, dependent_ssns = get_dependents_member_details(client_id, ssn_list, 'dependent_id', enrollment_type_change)
        db_dependents_member_records.update(dependents_records)
        employee_list = [record['employee_id'] for record in db_dependents_member_records.values()]
        ssn_list = list(set(ssn_list) - set(dependent_member_ssns))

        if ssn_list:
            ssn_list += employee_list
            primary_records, primary_ssns = get_primary_member_details(client_id, ssn_list, 'member_id', enrollment_type_change)
            db_primary_member_records.update(primary_records)

    return [db_dependents_member_records, db_primary_member_records]



