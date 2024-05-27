import datetime
import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Table, MetaData
import pandas as pd

from constants import connection_string

engine = create_engine(connection_string, pool_size=20, max_overflow=0)  # Enable connection pooling
metadata = MetaData()
metadata.reflect(bind=engine)

# Get references to the tables
tbl_ph_claims = metadata.tables['tbl_ph_claims']
tbl_ph_employer_info = metadata.tables['tbl_ph_employer_info']
tbl_ph_med_field = metadata.tables['tbl_ph_med_field']

def insert_data(all_valid_records_df, generated_records, engine, metadata, client_id, user_id, member_records):
    Session = sessionmaker(bind=engine)
    session = Session()

    with engine.connect() as conn:
        try:
            # Inserting data into tbl_ph_employer_info in batches
            if generated_records:
                employer_info_data = [{
                    'client_id': int(record['client_id']),
                    'employer_id': record['employer_id'] if record['employer_id'] != 'nan' else None,
                    'employer_name': record['employer_name'],
                    'employer_middle_name': record.get('employer_middle_name'),
                    'employer_last_name': record.get('employer_last_name'),
                    'status': record.get('status', 1),
                    'created_at': datetime.datetime.now(),
                    'created_by': int(record.get('created_by', 0)),
                    'updated_at': datetime.datetime.now(),
                    'updated_by': int(record.get('updated_by', 0))
                } for record in generated_records]

                employer_info_df = pd.DataFrame(employer_info_data)
                employer_info_df.to_sql('tbl_ph_employer_info', con=engine, if_exists='append', index=False, chunksize=10000)

                # Retrieve the inserted employer IDs
                inserted_employer_ids_df = pd.read_sql(
                    tbl_ph_employer_info.select().where(tbl_ph_employer_info.c.client_id == int(client_id)),
                    con=engine
                )
                employer_id_map = dict(zip(inserted_employer_ids_df['employer_id'], inserted_employer_ids_df['id']))

            active_member_records = member_records.get('db_primary_member_records', member_records.get('db_dependents_member_records', {}))

            # Prepare data for tbl_ph_claims
            bulk_size = 10000
            for i in range(0, len(all_valid_records_df), bulk_size):
                claims_data = []
                med_field_data_list = []
                for row in all_valid_records_df.iloc[i:i + bulk_size].itertuples(index=False):
                    unique_patient_id = str(row.UNIQUE_PATIENT_ID).lower()
                    member_record = active_member_records.get(unique_patient_id)
                    claim_record = {
                        'id': None,
                        'employer_id': employer_id_map.get(str(row.EMPLOYER_ID)),
                        'claim_status': row.CLAIM_STATUS,
                        'claim_type': row.CLAIM_TYPE,
                        'client_id': int(client_id),
                        'import_id': None,
                        'subscriber_id': None,
                        'patient_ssn': row.PATIENT_SSN,
                        'unique_patient_id': row.UNIQUE_PATIENT_ID,
                        'sir_id': None,
                        'original_unique_patient_id': member_record['original_id'] if member_record else None,
                        'subscriber_type': member_record['subscriber_type'] if member_record else None,
                        'employee_status': None,
                        'gender': member_record['gender'] if member_record else None,
                        'dob': member_record['dob'] if member_record else None,
                        'age': None,
                        'state': member_record['state'] if member_record else None,
                        'address': member_record['address'] if member_record else None,
                        'city': member_record['city'] if member_record else None,
                        'zip': member_record['zip'] if member_record else None,
                        'latitude': member_record['latitude'] if member_record else None,
                        'longitude': member_record['longitude'] if member_record else None,
                        'member_name': member_record['member_name'] if member_record else None,
                        'member_id': member_record['member_id'] if member_record else None,
                        'inpatient_or_outpatient': getattr(row, 'INPATIENT_OR_OUTPATIENT', None),
                        'claim_cause': getattr(row, 'CLAIM_CAUSE', None),
                        'benefit_code': getattr(row, 'BENEFIT_CODE', None),
                        'network': getattr(row, 'NETWORK', None),
                        'provider_name': getattr(row, 'PROVIDER_NAME', None),
                        'provider_paid_name': getattr(row, 'PROVIDER_PAID_NAME', None),
                        'ucr': getattr(row, 'UCR', None),
                        'cpt_modifier': getattr(row, 'CPT_MODIFIER', None),
                        'diagnosis_2': getattr(row, 'DIAGNOSIS_2', '00000000') if pd.notna(getattr(row, 'DIAGNOSIS_2', None)) else '00000000',
                        'diagnosis_3': getattr(row, 'DIAGNOSIS_3', '00000000') if pd.notna(getattr(row, 'DIAGNOSIS_3', None)) else '00000000',
                        'diagnosis_4': getattr(row, 'DIAGNOSIS_4', '00000000') if pd.notna(getattr(row, 'DIAGNOSIS_4', None)) else '00000000',
                        'diagnosis_5': getattr(row, 'DIAGNOSIS_5', '00000000') if pd.notna(getattr(row, 'DIAGNOSIS_5', None)) else '00000000',
                        'member_deductible_amount': getattr(row, 'MEMBER_DEDUCTIBLE_AMOUNT', None),
                        'member_oop_amount': getattr(row, 'MEMBER_OOP_AMOUNT', None),
                        'member_copay_amount': getattr(row, 'MEMBER_COPAY_AMOUNT', None),
                        'claim_number': getattr(row, 'CLAIM_NUMBER', None),
                        'claim_received_date': getattr(row, 'CLAIM_RECEIVED_DATE', None),
                        'claim_entry_date': getattr(row, 'CLAIM_ENTRY_DATE', None),
                        'adjuster': None,
                        'document_number': None,
                        'sequence': None,
                        'check_number': getattr(row, 'CHECK_NUMBER', None),
                        'benefits_assigned': getattr(row, 'BENEFITS_ASSIGNED', None),
                        'revenue_code': getattr(row, 'REVENUE_CODE', None),
                        'provider_ein': getattr(row, 'PROVIDER_EIN', None),
                        'provider_paid_npi': getattr(row, 'PROVIDER_PAID_NPI', None),
                        'provider_paid_zip': getattr(row, 'PROVIDER_PAID_ZIP', None),
                        'original_diagnosis_code': getattr(row, 'ORIGINAL_DIAGNOSIS_CODE', None),
                        'original_provider_code': getattr(row, 'ORIGINAL_PROVIDER_CODE', None),
                        'original_procedure_code': getattr(row, 'ORIGINAL_PROCEDURE_CODE', None),
                        'original_pos_code': getattr(row, 'ORIGINAL_POS_CODE', None),
                        'service_type': None,
                        'service_date': None,
                        'service_date_to': None,
                        'diagnostic_code': None,
                        'procedure_code': None,
                        'ndc_code': None,
                        'provider': None,
                        'place_of_service': int(getattr(row, 'PLACE_OF_SERVICE', 0)) if pd.notna(getattr(row, 'PLACE_OF_SERVICE', None)) else 0,
                        'network_indicator': None,
                        'service_code': None,
                        'total_charges': None,
                        'amount_allowed': None,
                        'total_paid': None,
                        'access_fee': None,
                        'paid_date': None,
                        'plan_paid_amount': getattr(row, 'PLAN_PAID_AMOUNT', None),
                        'is_preventive': None,
                        'facility_name': None,
                        'provider_type': None,
                        'location_code': getattr(row, 'LOCATION_CODE', None),
                        'sub_group_code': getattr(row, 'SUB_GROUP_CODE', None),
                        'plan_code': getattr(row, 'PLAN_CODE', None),
                        'created_at': datetime.datetime.now(),
                        'created_by': int(user_id),
                        'updated_at': datetime.datetime.now(),
                        'updated_by': int(user_id),
                        'admit_date': getattr(row, 'ADMIT_DATE', None),
                        'discharge_date': getattr(row, 'DISCHARGE_DATE', None),
                        'admission_days': getattr(row, 'ADMISSION_DAYS', None),
                        'discharge_status_code': getattr(row, 'DISCHARGE_STATUS_CODE', None),
                        'point_of_origin_code': getattr(row, 'POINT_OF_ORIGIN_CODE', None),
                        'admission_diagnosis_code': getattr(row, 'ADMISSION_DIAGNOSIS_CODE', None),
                        'patient_reason_diagnosis_code': getattr(row, 'PATIENT_REASON_DIAGNOSIS_CODE', None),
                    }
                    claims_data.append(claim_record)
                    med_field_data_list.append({
                        'claim_form_type': getattr(row, 'CLAIM_FORM_TYPE', None),
                        'type_of_bill_code': getattr(row, 'TYPE_OF_BILL_CODE', None)
                    })

                if claims_data:
                    claims_df = pd.DataFrame(claims_data)
                    claims_df.to_sql('tbl_ph_claims', con=engine, if_exists='append', index=False, chunksize=10000)

                    # Retrieve the inserted claim IDs
                    last_insert_id_query = "SELECT MAX(id) as id FROM tbl_ph_claims"
                    last_inserted_id = pd.read_sql(last_insert_id_query, con=engine).iloc[0]['id']
                    inserted_claims_df = pd.read_sql(
                        f"SELECT id FROM tbl_ph_claims WHERE id > {last_inserted_id - len(claims_data)} AND client_id = {int(client_id)}",
                        con=engine
                    )
                    inserted_claims_df = inserted_claims_df['id'].tolist()

                    # Prepare data for tbl_ph_med_field using the inserted claim IDs
                    med_field_data = [{
                        'claim_id': claim_id,
                        'client_id': int(client_id),
                        'claim_form_type': med_record['claim_form_type'],
                        'type_of_bill_code': med_record['type_of_bill_code']
                    } for claim_id, med_record in zip(inserted_claims_df, med_field_data_list) if med_record['claim_form_type'] or med_record['type_of_bill_code']]

                    if med_field_data:
                        med_field_df = pd.DataFrame(med_field_data).dropna(
                            subset=['claim_form_type', 'type_of_bill_code'], how='all')
                        med_field_df.to_sql('tbl_ph_med_field', con=engine, if_exists='append', index=False, chunksize=10000)

            session.commit()
        except Exception as e:
            session.rollback()
            print("Error occurred during data insertion:", e)
        finally:
            session.close()
