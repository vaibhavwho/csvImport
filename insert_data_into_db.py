import datetime
from sqlalchemy.orm import sessionmaker
import numpy as np
from sqlalchemy import create_engine, Table, MetaData
import pandas as pd
import concurrent.futures
from constants import connection_string
from sqlalchemy import insert, select, func

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

    try:
        # Inserting data into tbl_ph_employer_info
        employer_info_data = [{
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
        } for record in generated_records]

        if employer_info_data:
            session.execute(tbl_ph_employer_info.insert(), employer_info_data)
            session.commit()

            # Retrieve the inserted IDs
            employer_ids = session.execute(
                select(tbl_ph_employer_info.c.id, tbl_ph_employer_info.c.employer_id)
                .where(tbl_ph_employer_info.c.client_id == int(client_id))
            ).fetchall()

            employer_id_map = {str(employer_id): inserted_id for inserted_id, employer_id in employer_ids}
        else:
            employer_id_map = {}

        active_member_records = member_records.get('db_primary_member_records', member_records.get('db_dependents_member_records', {}))

        num_workers = 4

        partition_size = len(all_valid_records_df) // num_workers
        partitions = [all_valid_records_df[i:i + partition_size] for i in range(0, len(all_valid_records_df), partition_size)]

        claims_data = []
        med_field_data_list = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(process_partition, employer_id_map, active_member_records, partition, client_id, user_id) for partition in partitions]

            for future in concurrent.futures.as_completed(futures):
                partition_claims_data, partition_med_field_data_list = future.result()
                claims_data.extend(partition_claims_data)
                med_field_data_list.extend(partition_med_field_data_list)

        if claims_data:
            session.execute(tbl_ph_claims.insert(), claims_data)
            session.commit()

            claim_ids = session.execute(
                select(tbl_ph_claims.c.id)
                .where(tbl_ph_claims.c.client_id == int(client_id))
            ).fetchall()

            inserted_claim_ids = [claim_id for claim_id, in claim_ids]

            # Attach claim_id to med_field_data_list before inserting
            for i, med_field_data in enumerate(med_field_data_list):
                med_field_data['claim_id'] = inserted_claim_ids[i]
                med_field_data['client_id'] = int(client_id)

            if med_field_data_list:
                session.execute(tbl_ph_med_field.insert(), med_field_data_list)
                session.commit()

    except Exception as e:
        session.rollback()
        print("Error occurred during data insertion:", e)
    finally:
        session.close()


def process_partition(employer_id_map, active_member_records, partition, client_id, user_id):
    current_time = datetime.datetime.now()
    partition = partition.copy()

    partition['client_id'] = int(client_id)
    partition['created_at'] = current_time
    partition['created_by'] = int(user_id)
    partition['updated_at'] = current_time
    partition['updated_by'] = int(user_id)

    partition['original_unique_patient_id'] = partition['UNIQUE_PATIENT_ID'].map(lambda x: active_member_records.get(str(x).lower(), {}).get('original'))
    partition['subscriber_type'] = partition['UNIQUE_PATIENT_ID'].map(lambda x: active_member_records.get(str(x).lower(), {}).get('subscriber_type'))
    partition['gender'] = partition['UNIQUE_PATIENT_ID'].map(lambda x: active_member_records.get(str(x).lower(), {}).get('gender'))
    partition['dob'] = partition['UNIQUE_PATIENT_ID'].map(lambda x: active_member_records.get(str(x).lower(), {}).get('dob'))
    partition['state'] = partition['UNIQUE_PATIENT_ID'].map(lambda x: active_member_records.get(str(x).lower(), {}).get('state'))
    partition['address'] = partition['UNIQUE_PATIENT_ID'].map(lambda x: active_member_records.get(str(x).lower(), {}).get('address'))
    partition['city'] = partition['UNIQUE_PATIENT_ID'].map(lambda x: active_member_records.get(str(x).lower(), {}).get('city'))
    partition['zip'] = partition['UNIQUE_PATIENT_ID'].map(lambda x: active_member_records.get(str(x).lower(), {}).get('zip'))
    partition['latitude'] = partition['UNIQUE_PATIENT_ID'].map(lambda x: active_member_records.get(str(x).lower(), {}).get('latitude'))
    partition['longitude'] = partition['UNIQUE_PATIENT_ID'].map(lambda x: active_member_records.get(str(x).lower(), {}).get('longitude'))
    partition['member_name'] = partition['UNIQUE_PATIENT_ID'].map(lambda x: active_member_records.get(str(x).lower(), {}).get('member_name'))
    partition['member_id'] = partition['UNIQUE_PATIENT_ID'].map(lambda x: active_member_records.get(str(x).lower(), {}).get('member_id'))
    partition['employer_id'] = partition['EMPLOYER_ID'].map(lambda x: employer_id_map.get(str(x)))
    partition['place_of_service'] = partition['PLACE_OF_SERVICE']

    if 'diagnosis_2' in partition.columns and partition['diagnosis_2'].dtype.name == 'category':
        partition['diagnosis_2'] = partition['diagnosis_2'].cat.add_categories(['00000000'])
    if 'diagnosis_3' in partition.columns and partition['diagnosis_3'].dtype.name == 'category':
        partition['diagnosis_3'] = partition['diagnosis_3'].cat.add_categories(['00000000'])
    if 'diagnosis_4' in partition.columns and partition['diagnosis_4'].dtype.name == 'category':
        partition['diagnosis_4'] = partition['diagnosis_4'].cat.add_categories(['00000000'])
    if 'diagnosis_5' in partition.columns and partition['diagnosis_5'].dtype.name == 'category':
        partition['diagnosis_5'] = partition['diagnosis_5'].cat.add_categories(['00000000'])
    partition['claim_status'] = None
    columns_to_map = [
        'employer_id',
        'claim_status',
        'claim_type',
        'client_id',
        'patient_ssn',
        'unique_patient_id',
        'original_unique_patient_id',
        'subscriber_type',
        'gender',
        'dob',
        'state',
        'address',
        'city',
        'zip',
        'latitude',
        'longitude',
        'member_name',
        'member_id',
        'inpatient_or_outpatient',
        'claim_cause',
        'benefit_code',
        'network',
        'provider_name',
        'provider_paid_name',
        'ucr',
        'cpt_modifier',
        'diagnosis_2',
        'diagnosis_3',
        'diagnosis_4',
        'diagnosis_5',
        'member_deductible_amount',
        'member_oop_amount',
        'member_copay_amount',
        'claim_number',
        'claim_received_date',
        'claim_entry_date',
        'check_number',
        'benefits_assigned',
        'revenue_code',
        'provider_ein',
        'provider_paid_npi',
        'provider_paid_zip',
        'original_diagnosis_code',
        'original_provider_code',
        'original_procedure_code',
        'original_pos_code',
        'place_of_service',
        'plan_paid_amount',
        'location_code',
        'sub_group_code',
        'plan_code',
        'created_at',
        'created_by',
        'updated_at',
        'updated_by',
        'admit_date',
        'discharge_date',
        'admission_days',
        'discharge_status_code',
        'point_of_origin_code',
        'admission_diagnosis_code',
        'patient_reason_diagnosis_code'
    ]

    for column in columns_to_map:
        partition[column] = partition[column].fillna(partition[column].name)

    if 'claim_form_type' not in partition.columns:
        partition['claim_form_type'] = np.nan
    if 'type_of_bill_code' not in partition.columns:
        partition['type_of_bill_code'] = np.nan

    med_field_data_list = partition[['claim_form_type', 'type_of_bill_code', 'client_id']]

    return partition.to_dict(orient='records'), med_field_data_list.to_dict(orient='records')
