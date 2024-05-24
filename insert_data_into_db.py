import datetime
import pdb
from sqlalchemy.orm import sessionmaker
import numpy as np
from sqlalchemy import create_engine, Table, MetaData
import pandas as pd

from constants import connection_string

engine = create_engine(connection_string)
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
            # Inserting data into tbl_ph_employer_info
            employer_info_data = []
            for record in generated_records:
                employer_info_data.append({
                    'id': None,
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
                })
            inserted_ids = []
            if employer_info_data:
                for data in employer_info_data:
                    # result = conn.execute(tbl_ph_employer_info.insert(), data)
                    result = session.execute(tbl_ph_employer_info.insert(), data)
                    inserted_id = result.inserted_primary_key[0]
                    inserted_id = result.lastrowid
                    print("Inserted ID:", inserted_id)
                    inserted_ids.append(inserted_id)

            # Create a mapping of original employer IDs to inserted IDs
            employer_id_map = {str(record['employer_id']): inserted_id for record, inserted_id in zip(employer_info_data, inserted_ids)}
            print("EMPLOYER ID MAP:", employer_id_map)
            if member_records['db_primary_member_records']:
                active_member_records = member_records['db_primary_member_records']
            elif member_records['db_dependents_member_records']:
                active_member_records = member_records['db_dependents_member_records']
            else:
                active_member_records = {}
            # Prepare data for tbl_ph_claims
            claims_data = []
            med_field_data_list = []
            for _, row in all_valid_records_df.iterrows():
                row = row.replace({np.nan: None})
                unique_patient_id = str(row['UNIQUE_PATIENT_ID']).lower()
                member_record = active_member_records.get(unique_patient_id)
                claim_record = {
                    'id': None,
                    'employer_id': employer_id_map.get(str(row['EMPLOYER_ID'])),
                    'claim_status': row['CLAIM_STATUS'],
                    'claim_type': row['CLAIM_TYPE'],
                    'client_id': int(client_id),
                    'import_id': None,
                    'subscriber_id': None,
                    'patient_ssn': row['PATIENT_SSN'],
                    'unique_patient_id': row['UNIQUE_PATIENT_ID'],
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
                    'inpatient_or_outpatient': row.get('INPATIENT_OR_OUTPATIENT'),
                    'claim_cause': row.get('CLAIM_CAUSE'),
                    'benefit_code': row.get('BENEFIT_CODE'),
                    'network': row.get('NETWORK'),
                    'provider_name': row.get('PROVIDER_NAME'),
                    'provider_paid_name': row.get('PROVIDER_PAID_NAME'),
                    'ucr': row.get('UCR'),
                    'cpt_modifier': row.get('CPT_MODIFIER'),
                    'diagnosis_2': row.get('DIAGNOSIS_2') if row.get('DIAGNOSIS_2') else '00000000',
                    'diagnosis_3': row.get('DIAGNOSIS_3') if row.get('DIAGNOSIS_2') else '00000000',
                    'diagnosis_4': row.get('DIAGNOSIS_4') if row.get('DIAGNOSIS_2') else '00000000',
                    'diagnosis_5': row.get('DIAGNOSIS_5') if row.get('DIAGNOSIS_2') else '00000000',
                    'member_deductible_amount': row.get('MEMBER_DEDUCTIBLE_AMOUNT'),
                    'member_oop_amount': row.get('MEMBER_OOP_AMOUNT'),
                    'member_copay_amount': row.get('MEMBER_COPAY_AMOUNT'),
                    'claim_number': row.get('CLAIM_NUMBER'),
                    'claim_received_date': row.get('CLAIM_RECEIVED_DATE'),
                    'claim_entry_date': row.get('CLAIM_ENTRY_DATE'),
                    'adjuster': None,
                    'document_number': None,
                    'sequence': None,
                    'check_number': row.get('CHECK_NUMBER'),
                    'benefits_assigned': row.get('BENEFITS_ASSIGNED'),
                    'revenue_code': row.get('REVENUE_CODE'),
                    'provider_ein': row.get('PROVIDER_EIN'),
                    'provider_paid_npi': row.get('PROVIDER_PAID_NPI'),
                    'provider_paid_zip': row.get('PROVIDER_PAID_ZIP'),
                    'original_diagnosis_code': row.get('ORIGINAL_DIAGNOSIS_CODE'),
                    'original_provider_code': row.get('ORIGINAL_PROVIDER_CODE'),
                    'original_procedure_code': row.get('ORIGINAL_PROCEDURE_CODE'),
                    'original_pos_code': row.get('ORIGINAL_POS_CODE'),
                    'service_type': None,
                    'service_date': None,
                    'service_date_to': None,
                    'diagnostic_code': None,
                    'procedure_code': None,
                    'ndc_code': None,
                    'provider': None,
                    'place_of_service': int(row['PLACE_OF_SERVICE']) if not pd.isna(row['PLACE_OF_SERVICE']) else 0,
                    'network_indicator': None,
                    'service_code': None,
                    'total_charges': None,
                    'amount_allowed': None,
                    'total_paid': None,
                    'access_fee': None,
                    'paid_date': None,
                    'plan_paid_amount': row.get('PLAN_PAID_AMOUNT'),
                    'is_preventive': None,
                    'facility_name': None,
                    'provider_type': None,
                    'location_code': row.get('LOCATION_CODE'),
                    'sub_group_code': row.get('SUB_GROUP_CODE'),
                    'plan_code': row.get('PLAN_CODE'),
                    'created_at': datetime.datetime.now(),
                    'created_by': int(user_id),
                    'updated_at': datetime.datetime.now(),
                    'updated_by': int(user_id),
                    'admit_date': row.get('ADMIT_DATE'),
                    'discharge_date': row.get('DISCHARGE_DATE'),
                    'admission_days': row.get('ADMISSION_DAYS'),
                    'discharge_status_code': row.get('DISCHARGE_STATUS_CODE'),
                    'point_of_origin_code': row.get('POINT_OF_ORIGIN_CODE'),
                    'admission_diagnosis_code': row.get('ADMISSION_DIAGNOSIS_CODE'),
                    'patient_reason_diagnosis_code': row.get('PATIENT_REASON_DIAGNOSIS_CODE'),
                }
                claims_data.append(claim_record)
                med_field_data_list.append({
                    'claim_form_type': row.get('CLAIM_FORM_TYPE'),
                    'type_of_bill_code': row.get('TYPE_OF_BILL_CODE')
                })

            inserted_claim_ids = []
            claim_id_map = {}
            if claims_data:
                for idx, data in enumerate(claims_data):
                    result = session.execute(tbl_ph_claims.insert(), data)
                    inserted_claim_id = result.inserted_primary_key[0]
                    print("Inserted Claim ID:", inserted_claim_id)
                    inserted_claim_ids.append(inserted_claim_id)

                    # Use the corresponding entry from med_field_data_list
                    med_field_data = med_field_data_list[idx]

                    if med_field_data['claim_form_type'] or med_field_data['type_of_bill_code']:
                        # Prepare data for tbl_ph_med_field
                        med_field_data = {
                            'med_field_id': None,
                            'claim_id': inserted_claim_id,
                            'client_id': int(client_id),
                            'claim_form_type': med_field_data['claim_form_type'],
                            'type_of_bill_code': med_field_data['type_of_bill_code']
                        }

                        # Inserting data into tbl_ph_med_field
                        try:
                            session.execute(tbl_ph_med_field.insert(), med_field_data)
                        except Exception as e:
                            print("Error inserting data into tbl_ph_med_field:", e)

            # Commit the transaction to persist changes
            session.commit()
        except Exception as e:
            session.rollback()
            print("Error occurred during data insertion:", e)
        finally:
            session.close()
