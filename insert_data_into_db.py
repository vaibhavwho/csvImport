import datetime
import multiprocessing
import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Table, MetaData, text
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from constants import connection_string

engine = create_engine(connection_string, pool_size=20, max_overflow=0)
metadata = MetaData()
metadata.reflect(bind=engine)


tbl_ph_claims = metadata.tables['tbl_ph_claims']
tbl_ph_employer_info = metadata.tables['tbl_ph_employer_info']
tbl_ph_med_field = metadata.tables['tbl_ph_med_field']


def process_chunk(chunk, employer_id_map, active_member_records, client_id, user_id):
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
        chunk.itertuples(index=False),
        chunk_claim_received_dates,
        chunk_claim_entry_dates,
        chunk_place_of_services,
        chunk_diagnosis_2,
        chunk_diagnosis_3,
        chunk_diagnosis_4,
        chunk_diagnosis_5,
        chunk_medical_records.itertuples(index=False)
    ):
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
            'original_unique_patient_id': member_record.get('original_id') if member_record else None,
            'subscriber_type': member_record.get('subscriber_type') if member_record else None,
            'employee_status': None,
            'gender': member_record.get('gender') if member_record else None,
            'dob': member_record.get('dob') if member_record else None,
            'age': None,
            'state': member_record.get('state') if member_record else None,
            'address': member_record.get('address') if member_record else None,
            'city': member_record.get('city') if member_record else None,
            'zip': member_record.get('zip') if member_record else None,
            'latitude': member_record.get('latitude') if member_record else None,
            'longitude': member_record.get('longitude') if member_record else None,
            'member_name': member_record.get('member_name') if member_record else None,
            'member_id': member_record.get('member_id') if member_record else None,
            'inpatient_or_outpatient': getattr(row, 'INPATIENT_OR_OUTPATIENT', None),
            'claim_cause': getattr(row, 'CLAIM_CAUSE', None),
            'benefit_code': getattr(row, 'BENEFIT_CODE', None),
            'network': getattr(row, 'NETWORK', None),
            'provider_name': getattr(row, 'PROVIDER_NAME', None),
            'provider_paid_name': getattr(row, 'PROVIDER_PAID_NAME', None),
            'ucr': getattr(row, 'UCR', None),
            'cpt_modifier': getattr(row, 'CPT_MODIFIER', None),
            'diagnosis_2': diagnosis_2 if pd.notna(diagnosis_2) else '00000000',
            'diagnosis_3': diagnosis_3 if pd.notna(diagnosis_3) else '00000000',
            'diagnosis_4': diagnosis_4 if pd.notna(diagnosis_4) else '00000000',
            'diagnosis_5': diagnosis_5 if pd.notna(diagnosis_5) else '00000000',
            'member_deductible_amount': getattr(row, 'MEMBER_DEDUCTIBLE_AMOUNT', None),
            'member_oop_amount': getattr(row, 'MEMBER_OOP_AMOUNT', None),
            'member_copay_amount': getattr(row, 'MEMBER_COPAY_AMOUNT', None),
            'claim_number': getattr(row, 'CLAIM_NUMBER', None),
            'claim_received_date': claim_received_date,
            'claim_entry_date': claim_entry_date,
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
            'place_of_service': int(place_of_service) if pd.notna(place_of_service) else 0,
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
            'claim_form_type': med_record.CLAIM_FORM_TYPE,
            'type_of_bill_code': med_record.TYPE_OF_BILL_CODE
        })

    claims_df = pd.DataFrame(claims_data)
    start_claim_insertion_time = datetime.datetime.now()
    claims_df.to_sql('tbl_ph_claims', con=engine, if_exists='append', index=False)
    end_claim_insertion_time = datetime.datetime.now()
    print("Time taken for claims insertion:", end_claim_insertion_time - start_claim_insertion_time)
    with engine.connect() as connection:
        result = connection.execute(text("SELECT LAST_INSERT_ID()"))
        last_id = result.scalar()

    inserted_claim_ids = list(range(last_id - len(claims_data) + 1, last_id + 1))

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
        start_med_field_insertion_time = datetime.datetime.now()
        med_field_df.to_sql('tbl_ph_med_field', con=engine, if_exists='append', index=False)
        end_med_field_insertion_time = datetime.datetime.now()
        print("Time taken for med field insertion:", end_med_field_insertion_time - start_med_field_insertion_time)


def insert_data(all_valid_records_df, generated_records, engine, metadata, client_id, user_id, member_records):
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
                    futures.append(executor.submit(process_chunk, chunk, employer_id_map, active_member_records, client_id, user_id))

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
