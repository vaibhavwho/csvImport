import datetime
import re
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from constants import connection_string
from save_filter import save_filter_array

# Setup database connection
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)
session = Session()

# Global dictionaries
db_dependents_member_records = {}
db_primary_member_records = {}
db_enrollment_records = {}
is_claims_import = False


def get_dependents_member_details(client_id, ssn_list, match_type, enrollment_type_change):
    global db_dependents_member_records

    if not isinstance(ssn_list, list):
        ssn_list = [ssn_list]

    filter_dependent = ''
    if ssn_list:
        ssn_query = save_filter_array(ssn_list, 'ssn_trigger', [], client_id)
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

    data_dependent = pd.read_sql_query(sql, con=engine)
    indexed_data = data_dependent.set_index('ssn').to_dict(orient='index')
    db_dependents_member_records.update(indexed_data)


def get_primary_member_details(client_id, ssn_list, match_type, enrollment_type_change):
    global db_primary_member_records, db_enrollment_records, is_claims_import

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

    data_primary = pd.read_sql_query(sql, con=engine)
    indexed_data = data_primary.set_index('ssn').to_dict(orient='index')

    for key in indexed_data:
        if 'subscriber_type' not in indexed_data[key]:
            indexed_data[key]['subscriber_type'] = 'EMPLOYEE'

    db_primary_member_records.update(indexed_data)

    print(f"Primary member records: {db_primary_member_records}")

    employee_ids = list(indexed_data.keys())
    get_enrollment_details(client_id, employee_ids)


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


def get_enrollment_details(client_id, employee_ids=[]):
    global db_enrollment_records, is_claims_import

    if not db_enrollment_records or is_claims_import:
        filter_enrollment = ''
        if employee_ids:
            if not isinstance(employee_ids, list):
                employee_ids = [employee_ids]
            employee = save_filter_array(employee_ids, 'ssn_trigger', [], client_id)
            filter_enrollment = f" AND employee_id IN ({employee})"

        sql = f"""
            SELECT 
                CASE 
                    WHEN lo.option_value = 'Active' THEN 'ACTIVE'
                    WHEN lo.option_value = 'Retired' THEN 'RETIRED'
                    WHEN lo.option_value = 'Terminated' THEN 'TERMINATED'
                    WHEN lo.option_value = 'COBRA' THEN 'COBRA'
                    ELSE '' 
                END AS employee_status,
                LOWER(employee_id) AS employee_id,
                coverage_month_start_day,
                coverage_month_end_day,
                (SELECT COUNT(*) FROM tbl_ph_employee_enrollments tc WHERE tc.employee_id = te.employee_id) AS total_count
            FROM 
                tbl_ph_employee_enrollments te
                INNER JOIN tbl_sir_lookup_options lo ON lo.option_id = te.employee_status AND lo.option_type = 14
            WHERE 
                client_id = {client_id} {filter_enrollment}
            ORDER BY 
                te.employee_id
        """

        data_enrollment = pd.read_sql_query(sql, con=engine)
        indexed_enroll = index_enrollment_records(data_enrollment) if not data_enrollment.empty else {}
        db_enrollment_records.update(indexed_enroll)


def index_enrollment_records(data_enrollment):
    records = {}
    total_enrollment = len(data_enrollment)
    i = 0

    while i < total_enrollment:
        individual_emp_count = data_enrollment.iloc[i]['total_count']
        emp_id = data_enrollment.iloc[i]['employee_id']
        sliced_array = data_enrollment.iloc[i:i + individual_emp_count].to_dict(orient='records')
        records[emp_id] = sliced_array
        i += individual_emp_count

    return records


def set_members_attributes(group, record_type, columns):
    global db_dependents_member_records, db_primary_member_records, is_claims_import
    try:
        ssn = group['UNIQUE_PATIENT_ID'].iloc[0].lower()
        employee_id = ''
        attributes_array = {}

        if ssn in db_primary_member_records:
            employee_id = ssn
            attributes_array = db_primary_member_records[employee_id]
        elif ssn in db_dependents_member_records:
            attributes_array = db_dependents_member_records[ssn]
            employee_id = attributes_array.get('employee_id', '')

        sir_id = generate_sir_id(attributes_array)
        group['SIR_ID'] = sir_id

        for column in columns:
            if column == 'employee_status':
                group[column] = group.apply(
                    lambda row: get_enrollment_status(row, record_type, employee_id), axis=1
                )
            else:
                group[column] = group[column].apply(
                    lambda x: attributes_array.get(column, '')
                )

        if is_claims_import:
            group['UNIQUE_PATIENT_ID'] = group.apply(
                lambda row: attributes_array.get('original_id', row['UNIQUE_PATIENT_ID']), axis=1
            )

    except KeyError as e:
        print(f"KeyError in set_members_attributes: Missing key {e} in attributes_array")
    except Exception as e:
        print(f"Error in set_members_attributes: {e}")
    return group


def get_enrollment_status(row, record_type, employee_id):
    global db_enrollment_records
    try:
        if not employee_id or employee_id not in db_enrollment_records:
            return ''

        claim_start_date = row['CLAIM_PAID_DATE'] if record_type == 'pharmacy' else row['PAID_DATE']
        claim_start_date = pd.to_datetime(claim_start_date).date()

        for enrollment in db_enrollment_records[employee_id]:
            coverage_start = pd.to_datetime(enrollment['coverage_month_start_day']).date()
            coverage_end = pd.to_datetime(enrollment['coverage_month_end_day']).date() if enrollment['coverage_month_end_day'] else None

            if claim_start_date >= coverage_start and (not coverage_end or claim_start_date <= coverage_end):
                return enrollment['employee_status']

    except Exception as e:
        print(f"Error in get_enrollment_status: {e}")

    return ''


def generate_sir_id(member_records):
    try:
        sir_id = None
        employee_ssn = member_records.get('employee_ssn')
        if employee_ssn and member_records:
            try:
                subscriber_type = member_records['subscriber_type'].lower()[0]
            except KeyError as e:
                print(f"KeyError in generate_sir_id: Missing key {e} in member_records")
                return None

            employee_ssn = re.sub(r'[^0-9]', '', employee_ssn)
            last4 = employee_ssn[-4:]
            middle2 = employee_ssn[-6:-4]
            first3 = employee_ssn[-9:-6]

            member_name = member_records.get('member_name', '').split()[0]
            member_name = re.sub(r'[^A-Za-z0-9]', '', member_name).lower()
            name = member_name[:4]

            sir_id = f"{subscriber_type}{last4}{middle2}{name}{first3}"

    except Exception as e:
        print(f"Error in generate_sir_id: {e}")

    return sir_id


