from datetime import datetime, timedelta
import random
import time
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from constants import IN_DB_CONDITION, SSNTRIGGER, CONDITIONS, isMajorCategory, isSubDiagnosis, isChronic, \
    isTriggerDiagnosis, PROCEDURES, PROVIDERS, MEMBERS, LOADDATA, connection_string

engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)


def save_filter_array(filters, parent_type, applied_filters, client_id=None):
    # print("Starting save_filter_array function")
    is_manual = True
    try:
        if len(filters) <= 20 or not IN_DB_CONDITION:
            # print("Filters count <= 20 or IN_DB_CONDITION is empty, returning filters as string")
            return '"' + '","'.join(filters) + '"'
        else:
            filter_type = None
            session_id = f'AUTO_{client_id}_{int(time.time())}_{random.randint(10000, 99999)}'
            expiry_time = (datetime.now() + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')

            # Determine the filter type based on parent type and applied filters
            if parent_type == 'ssn_trigger':
                filter_type = 'ALL'
            elif parent_type == 'condition':
                if applied_filters.get('MAJOR CATEGORY'):
                    filter_type = isMajorCategory
                elif applied_filters.get('SUBDIAGNOSIS'):
                    filter_type = isSubDiagnosis
                elif applied_filters.get('CHRONIC CONDITIONS'):
                    filter_type = isChronic
                elif applied_filters.get('TRIGGER DIAGNOSIS'):
                    filter_type = isTriggerDiagnosis
            elif parent_type == 'procedure':
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
            elif parent_type == 'provider':
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
            elif parent_type == 'member':
                filter_type = 'ALL MEMBERS'
                if applied_filters.get('specificDeductibeAmount'):
                    filter_type = 'SHOCK CLAIMS'
                elif applied_filters.get('isChronic'):
                    filter_type = 'CHRONIC CONDITION'
                elif applied_filters.get('isMemberChronic'):
                    filter_type = 'CHRONIC CONDITION MEMBERS'
            elif parent_type == 'loadData':
                filter_type = applied_filters.get('filterType')

            # Prepare the filter values for insertion
            filters = ','.join([f'("{session_id}", "{filter_type}", "{expiry_time}", "{f}")' for f in filters])

            # print("Filter values prepared:", filters)

            # Create a database session
            db_session = Session()
            # print("Database session created")

            # If manual, delete existing records with the same filter type and session id
            if is_manual:
                # print("Manual deletion of existing records")
                db_session.execute(text(
                    f"DELETE FROM tbl_meta_filter_{parent_type}_values WHERE filter_type = '{filter_type}' AND session_id= '{session_id}'")),

                # print("Manual deletion completed")

            # print("Inserting new filters into the database")
            db_session.execute(text(
                f"INSERT INTO tbl_meta_filter_{parent_type}_values (session_id, filter_type, expire, filter_values) VALUES {filters}"))
            db_session.commit()
            # print("New filters inserted")

            if parent_type == 'loadData':
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
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None


def filter_garbage_remover():
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    Session = sessionmaker(bind=engine)
    db_session = Session()
    db_session.execute(text(f"DELETE FROM tbl_meta_filter_{CONDITIONS}_values WHERE expire <= :current_time"), {'current_time': current_time})
    db_session.execute(text(f"DELETE FROM tbl_meta_filter_{PROCEDURES}_values WHERE expire <= :current_time"), {'current_time': current_time})
    db_session.execute(text(f"DELETE FROM tbl_meta_filter_{PROVIDERS}_values WHERE expire <= :current_time"), {'current_time': current_time})
    db_session.execute(text(f"DELETE FROM tbl_meta_filter_{MEMBERS}_values WHERE expire <= :current_time"), {'current_time': current_time})
    db_session.execute(text(f"DELETE FROM tbl_meta_filter_{SSNTRIGGER}_values WHERE expire <= :current_time"), {'current_time': current_time})
    db_session.execute(text("DELETE FROM tbl_meta_member_chronic_compliance_dynamic"))
    db_session.commit()

filter_garbage_remover()