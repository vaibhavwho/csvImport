from datetime import datetime, timedelta
import random
import time
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from constants import IN_DB_CONDITION, SSNTRIGGER, CONDITIONS, isMajorCategory, isSubDiagnosis, isChronic, \
    isTriggerDiagnosis, PROCEDURES, PROVIDERS, MEMBERS, LOADDATA, connection_string

engine = create_engine(connection_string)

def save_filter_array(filters, parent_type, applied_filters, client_id=None):
    is_manual = True

    # Check if filter count is less than or equal to 20
    if len(filters) <= 20 or not IN_DB_CONDITION:
        return '"' + '","'.join(filters) + '"'
    else:
        filter_type = None
        session_id = f'AUTO_{client_id}_{int(time.time())}_{random.randint(10000, 99999)}'
        expiry_time = (datetime.now() + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')

        # Determine the filter type based on parent type and applied filters
        if parent_type == SSNTRIGGER:
            filter_type = 'ALL'
        elif parent_type == CONDITIONS:
            if applied_filters.get('isMajorCategory'):
                filter_type = isMajorCategory
            elif applied_filters.get('isSubDiagnosis'):
                filter_type = isSubDiagnosis
            elif applied_filters.get('isChronic'):
                filter_type = isChronic
            elif applied_filters.get('isTriggerDiagnosis'):
                filter_type = isTriggerDiagnosis
        elif parent_type == PROCEDURES:
            filter_type = 'ALL PROCEDURES'
            if 'mainhref' in applied_filters.get('args', {}):
                mainhref = applied_filters['args']['mainhref']
                filter_type = {
                    'hospital': 'INPATIENT',
                    'outpatient': 'OUTPATIENT',
                    'er': 'ER',
                    'ur': 'UR',
                    'officevisit': 'OFFICE',
                    'telehealth': 'TELEHEALTH',
                    'labs': 'LABS',
                    'imaging': 'IMAGING',
                    'other': 'OTHERS',
                    'pharmacy': 'SPECIALITY RX' if applied_filters['args'].get('pharmacyrx') else 'ALL PHARMACY',
                    'preventive': 'PREVENTIVE',
                    'procedureChronic': 'CHRONIC CONDITION'
                }.get(mainhref, filter_type)
        elif parent_type == PROVIDERS:
            filter_type = 'ALL PROVIDERS'
            if 'purpose' in applied_filters:
                purpose = applied_filters['purpose']
                filter_type = {
                    'all': 'ALL PROVIDERS',
                    'hospital': 'INPATIENT',
                    'outpatient': 'OUTPATIENT',
                    'er': 'ERUR',
                    'officevisit': 'OFFICE',
                    'pharmacy': 'PHARMACY'
                }.get(purpose, filter_type)
        elif parent_type == MEMBERS:
            filter_type = 'ALL MEMBERS'
            if applied_filters.get('specificDeductibeAmount'):
                filter_type = 'SHOCK CLAIMS'
            elif applied_filters.get('isChronic'):
                filter_type = 'CHRONIC CONDITION'
            elif applied_filters.get('isMemberChronic'):
                filter_type = 'CHRONIC CONDITION MEMBERS'
        elif parent_type == LOADDATA:
            filter_type = applied_filters.get('filterType')

        # Prepare the filter values for insertion
        filters = ','.join([f'("{session_id}", "{filter_type}", "{expiry_time}", "{f}")' for f in filters])

        # Create a database session
        Session = sessionmaker(bind=engine)
        db_session = Session()

        # If manual, delete existing records with the same filter type and session id
        if is_manual:
            db_session.execute(text(
                f"DELETE FROM tbl_meta_filter_{parent_type}_values WHERE filter_type = :filter_type AND session_id= :session_id"),
                {'filter_type': filter_type, 'session_id': session_id})

        # Insert the new filters
        db_session.execute(text(
            f"INSERT INTO tbl_meta_filter_{parent_type}_values (session_id, filter_type, expire, filter_values) VALUES {filters}"))
        db_session.commit()

        # Return appropriate query based on parent type and filter type
        if parent_type == LOADDATA:
            if filter_type == 'MEDICAL':
                return f"""
                SELECT sum(b.plan_paid_amount) as paid_amount, sum(b.total_charges) as total_charges, a.filter_values 
                FROM tbl_meta_filter_loadData_values a
                LEFT JOIN tbl_ph_claims b ON a.filter_values = b.import_id
                WHERE filter_type = '{filter_type}' AND session_id= '{session_id}'
                GROUP BY a.filter_values
                """
            else:
                return f"""
                SELECT sum(b.plan_paid_amount) as paid_amount, a.filter_values 
                FROM tbl_meta_filter_loadData_values a
                LEFT JOIN tbl_ph_pharmacy b ON a.filter_values = b.import_id
                WHERE filter_type = '{filter_type}' AND session_id= '{session_id}'
                GROUP BY a.filter_values
                """
        else:
            return f"SELECT filter_values FROM tbl_meta_filter_{parent_type}_values WHERE filter_type = '{filter_type}' AND session_id= '{session_id}'"

# def filter_garbage_remover():
#     current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     Session = sessionmaker(bind=engine)
#     db_session = Session()
#     db_session.execute(text(f"DELETE FROM tbl_meta_filter_{CONDITIONS}_values WHERE expire <= :current_time"), {'current_time': current_time})
#     db_session.execute(text(f"DELETE FROM tbl_meta_filter_{PROCEDURES}_values WHERE expire <= :current_time"), {'current_time': current_time})
#     db_session.execute(text(f"DELETE FROM tbl_meta_filter_{PROVIDERS}_values WHERE expire <= :current_time"), {'current_time': current_time})
#     db_session.execute(text(f"DELETE FROM tbl_meta_filter_{MEMBERS}_values WHERE expire <= :current_time"), {'current_time': current_time})
#     db_session.execute(text(f"DELETE FROM tbl_meta_filter_{SSNTRIGGER}_values WHERE expire <= :current_time"), {'current_time': current_time})
#     db_session.execute(text("DELETE FROM tbl_meta_member_chronic_compliance_dynamic"))
#     db_session.commit()
