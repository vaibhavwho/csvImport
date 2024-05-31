import datetime
import multiprocessing
import pdb

import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Table, MetaData, text
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from constants import connection_string
from get_all_member_records import get_enrollment_status

engine = create_engine(connection_string, pool_size=20, max_overflow=0)
metadata = MetaData()
metadata.reflect(bind=engine)


tbl_ph_claims = metadata.tables['tbl_ph_claims']
tbl_ph_employer_info = metadata.tables['tbl_ph_employer_info']
tbl_ph_med_field = metadata.tables['tbl_ph_med_field']


def process_chunk(chunk, employer_id_map, active_member_records, client_id, user_id,
                  provider_code_list, procedure_code_list, diagnostic_code_list, place_of_service_dict):
    claims_data = []
    med_field_data_list = []

    chunk_unique_patient_ids = chunk['UNIQUE_PATIENT_ID'].str.lower()
    chunk_claim_received_dates = chunk['CLAIM_RECEIVED_DATE']
    chunk_claim_entry_dates = chunk['CLAIM_ENTRY_DATE']
    chunk_place_of_services = chunk['PLACE_OF_SERVICE']
    chunk_diagnosis_2 = chunk['DIAGNOSIS_2']
    chunk_diagnosis_3 = chunk['DIAGNOSIS_3']
    chunk_diagnosis_4 = chunk['DIAGNOSIS_4']
    chunk_diagnosis_5 = chunk['DIAGNOSIS_5']
    chunk_medical_records = chunk[['CLAIM_FORM_TYPE', 'TYPE_OF_BILL_CODE']]

    for unique_patient_id, row, claim_received_date, claim_entry_date, place_of_service, diagnosis_2, diagnosis_3, diagnosis_4, diagnosis_5, med_record in zip(
        chunk_unique_patient_ids,
        chunk.itertuples(index=False, name='Row'),
        chunk_claim_received_dates,
        chunk_claim_entry_dates,
        chunk_place_of_services,
        chunk_diagnosis_2,
        chunk_diagnosis_3,
        chunk_diagnosis_4,
        chunk_diagnosis_5,
        chunk_medical_records.itertuples(index=False, name='MedRecord')
    ):
        member_record = active_member_records.get(unique_patient_id)
        employee_status = get_enrollment_status(row._asdict(), 'medical', unique_patient_id)
        provider_npi = row.PROVIDER_NPI
        provider_code = provider_code_list.get(provider_npi)

        cpt_procedure = row.CPT_PROCEDURE
        procedure_code = procedure_code_list.get(cpt_procedure)

        diagnosis_1 = row.DIAGNOSIS_1
        diagnostic_code = diagnostic_code_list.get(diagnosis_1)

        place_of_service_code = int(place_of_service) if pd.notna(place_of_service) else None
        place_of_service_value = place_of_service_dict.get(place_of_service_code)

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
            'sir_id': member_record.get('sir_id') if member_record else None,
            'original_unique_patient_id': member_record.get('original_id') if member_record else None,
            'subscriber_type': member_record.get('subscriber_type') if member_record else None,
            'employee_status': employee_status,
            'gender': member_record.get('gender') if member_record else None,
            'dob': member_record.get('dob') if member_record else None,
            'age': member_record.get('age') if member_record else None,
            'state': member_record.get('state') if member_record else None,
            'address': member_record.get('address') if member_record else None,
            'city': member_record.get('city') if member_record else None,
            'zip': member_record.get('zip') if member_record else None,
            'latitude': member_record.get('latitude') if member_record else None,
            'longitude': member_record.get('longitude') if member_record else None,
            'member_name': member_record.get('member_name') if member_record else None,
            'member_id': member_record.get('member_id') if member_record else None,
            'inpatient_or_outpatient': row.INPATIENT_OR_OUTPATIENT,
            'claim_cause': row.CLAIM_CAUSE,
            'benefit_code': row.BENEFIT_CODE,
            'network': row.NETWORK,
            'provider_name': row.PROVIDER_NAME,
            'provider_paid_name': row.PROVIDER_PAID_NAME,
            'ucr': row.UCR,
            'cpt_modifier': row.CPT_MODIFIER,
            'diagnosis_2': diagnosis_2 if pd.notna(diagnosis_2) else '00000000',
            'diagnosis_3': diagnosis_3 if pd.notna(diagnosis_3) else '00000000',
            'diagnosis_4': diagnosis_4 if pd.notna(diagnosis_4) else '00000000',
            'diagnosis_5': diagnosis_5 if pd.notna(diagnosis_5) else '00000000',
            'member_deductible_amount': row.MEMBER_DEDUCTIBLE_AMOUNT,
            'member_oop_amount': row.MEMBER_OOP_AMOUNT,
            'member_copay_amount': row.MEMBER_COPAY_AMOUNT,
            'claim_number': row.CLAIM_NUMBER,
            'claim_received_date': claim_received_date,
            'claim_entry_date': claim_entry_date,
            'adjuster': row.REMARKS_CODE_1,
            'document_number': row.REMARKS_CODE_2,
            'sequence': row.REMARKS_CODE_3,
            'check_number': row.CHECK_NUMBER,
            'benefits_assigned': row.BENEFITS_ASSIGNED,
            'revenue_code': row.REVENUE_CODE,
            'provider_ein': row.PROVIDER_EIN,
            'provider_paid_npi': row.PROVIDER_PAID_NAME,
            'provider_paid_zip': row.PROVIDER_PAID_ZIP,
            'original_diagnosis_code': row.ORIGINAL_DIAGNOSIS_CODE,
            'original_provider_code': row.ORIGINAL_PROVIDER_CODE,
            'original_procedure_code': row.ORIGINAL_PROCEDURE_CODE,
            'original_pos_code': row.ORIGINAL_POS_CODE,
            'service_type': None,
            'service_date': row.SERVICE_START_DATE,
            'service_date_to': row.SERVICE_END_DATE,
            'diagnostic_code': diagnostic_code,
            'procedure_code': procedure_code,
            'ndc_code': None,
            'provider': provider_code,
            'place_of_service': place_of_service_value,
            'network_indicator': None,
            'service_code': None,
            'total_charges': row.CHARGED_AMOUNT,
            'amount_allowed': None,
            'total_paid': row.COVERED_AMOUNT,
            'access_fee': None,
            'paid_date': row.CLAIM_PAID_DATE,
            'plan_paid_amount': row.PLAN_PAID_AMOUNT,
            'is_preventive': None,
            'facility_name': None,
            'provider_type': None,
            'location_code': row.LOCATION_CODE,
            'sub_group_code': row.SUB_GROUP_CODE,
            'plan_code': row.PLAN_CODE,
            'created_at': datetime.datetime.now(),
            'created_by': int(user_id),
            'updated_at': datetime.datetime.now(),
            'updated_by': int(user_id),
            'admit_date': row.ADMIT_DATE,
            'discharge_date': row.DISCHARGE_DATE,
            'admission_days': row.ADMISSION_DAYS,
            'discharge_status_code': row.DISCHARGE_STATUS_CODE,
            'point_of_origin_code': row.POINT_OF_ORIGIN_CODE,
            'admission_diagnosis_code': row.ADMISSION_DIAGNOSIS_CODE,
            'patient_reason_diagnosis_code': row.PATIENT_REASON_DIAGNOSIS_CODE,
        }
        claims_data.append(claim_record)
        med_field_data_list.append({
            'claim_form_type': med_record.CLAIM_FORM_TYPE,
            'type_of_bill_code': med_record.TYPE_OF_BILL_CODE
        })

    claims_df = pd.DataFrame(claims_data)
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            claims_df.to_sql('tbl_ph_claims', con=connection, if_exists='append', index=False)

            inserted_claim_ids = connection.execute(
                text("""
                    SELECT id FROM tbl_ph_claims 
                    WHERE created_by = :user_id 
                    AND client_id = :client_id 
                    ORDER BY created_at DESC 
                    LIMIT :count
                """),
                {'user_id': user_id, 'client_id': client_id, 'count': len(claims_data)}
            ).fetchall()

            inserted_claim_ids = [row[0] for row in inserted_claim_ids][::-1]

            if len(inserted_claim_ids) != len(claims_data):
                raise ValueError("Mismatch between inserted claims and claims data")

            med_field_data = [{
                'claim_id': claim_id,
                'client_id': int(client_id),
                'claim_form_type': med_field['claim_form_type'],
                'type_of_bill_code': med_field['type_of_bill_code']
            } for claim_id, med_field in zip(inserted_claim_ids, med_field_data_list)
                if med_field['claim_form_type'] or med_field['type_of_bill_code']]

            if med_field_data:
                med_field_df = pd.DataFrame(med_field_data).dropna(
                    subset=['claim_form_type', 'type_of_bill_code'], how='all')
                med_field_df.to_sql('tbl_ph_med_field', con=connection, if_exists='append', index=False)

            transaction.commit()
        except Exception as e:
            transaction.rollback()
            print("Error occurred during data insertion:", e)
            raise

def insert_data(all_valid_records_df, generated_records, engine, metadata, client_id, user_id, member_records,
                provider_code_list, procedure_code_list, diagnostic_code_list, place_of_service):
    Session = sessionmaker(bind=engine)
    session = Session()
    with engine.connect() as conn:
        try:
            start_time = datetime.datetime.now()
            print("Inserting data into tbl_ph_employer_info")
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
            print("Inserting data into tbl_ph_employer_info finished")
            bulk_size = 33000
            cpu_count = multiprocessing.cpu_count()
            with ThreadPoolExecutor(max_workers=cpu_count) as executor:
                futures = []
                for i in range(0, len(all_valid_records_df), bulk_size):
                    chunk = all_valid_records_df.iloc[i:i + bulk_size]
                    start_chunk_time = datetime.datetime.now()
                    futures.append(
                        executor.submit(process_chunk, chunk, employer_id_map, active_member_records, client_id, user_id,
                                        provider_code_list, procedure_code_list, diagnostic_code_list, place_of_service))

                for future in as_completed(futures):
                    future.result()
                    end_chunk_time = datetime.datetime.now()
                    print("Time taken for processing chunk:", end_chunk_time - start_chunk_time)

            end_time = datetime.datetime.now()
            print("Total time taken for data insertion:", end_time - start_time)
            session.commit()
        except Exception as e:
            session.rollback()
            print("Error occurred during data insertion:", e)
        finally:
            session.close()

