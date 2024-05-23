import os
import pdb
import re
import time
from flask import Flask, request, jsonify, g
from pathlib import Path
import pandas as pd
import pandera as pa
from pandera import DataFrameSchema, Column, Check
import multiprocessing as mp
import concurrent.futures
from dask import dataframe as dd
from dask.distributed import Client, LocalCluster
import dask
from constants import SERVICE_TYPE
from custom_dataframe_schema import create_schema
from get_all_member_records import get_all_members_records
from get_info import get_employer_dataframe, get_provider_dataframe
from get_options import get_lookup_option, get_diagnostic_code_list, get_provider_code_list_upload, \
    get_procedure_code_list, get_benefit_code_list_array, get_place_of_service
from validate_employer_id import validate_employer_id


def validate_chunk(schema, chunk, start_index):
    print(f"Process {os.getpid()} started for chunk.")
    errors = []
    validated_records_df = pd.DataFrame()
    error_records_df = pd.DataFrame()
    # error_row_number = {}
    try:
        validated_chunk = schema.validate(chunk, lazy=True)
        validated_records_df = pd.concat([validated_records_df, validated_chunk], ignore_index=True)
    except pa.errors.SchemaErrors as e:
        for error in e.failure_cases.to_dict(orient='records'):
            error_index = error['index']
            row_number = start_index + error_index if error_index is not None else None
            errors.append({
                "index": error_index,
                "column": error['column'],
                "error": error['failure_case'],
                "row_number": row_number
            })
            # error_row_number.update(row_number)
        # error_records_df = pd.concat([error_records_df, chunk.loc[error_row_number]], ignore_index=True)
        erroneous_indices = e.failure_cases['index'].tolist()
        error_records_df = pd.concat([error_records_df, chunk.loc[erroneous_indices]], ignore_index=True)
        valid_chunk = chunk.drop(index=erroneous_indices)
        validated_records_df = pd.concat([validated_records_df, valid_chunk], ignore_index=True)

    print(f"Process {os.getpid()} finished for chunk.")
    return validated_records_df, errors, error_records_df


def create_app():
    app = Flask(__name__)

    @app.route('/process_file', methods=['POST'])
    def validate_csv():
        start_time = time.time()
        request_data = request.json
        file_path = request_data.get('file_path')
        client_id = request_data.get('client_id')
        user_id = request_data.get('user_id')
        type = request_data.get('type')

        if not file_path:
            return jsonify({"error": "File path not provided"}), 400

        path = Path(file_path)
        if not path.exists():
            return jsonify({"error": "File not found"}), 400

        try:
            print("Reading CSV file in parallel using Dask...")
            ddf = dd.read_csv(
                path,
                # converters={
                #     'PLAN_PAID_AMOUNT': to_float
                # },
                dtype={
                    'EMPLOYER_ID': 'category',
                    'EMPLOYER_NAME': 'category',
                    'CLAIM_STATUS': 'category',
                    'CLAIM_TYPE': 'category',
                    'SERVICE_START_DATE': 'object',
                    'SERVICE_END_DATE': 'object',
                    'PROVIDER_NPI': 'category',
                    'PLACE_OF_SERVICE': 'category',
                    'CPT_PROCEDURE': 'category',
                    'DIAGNOSIS_1': 'category',
                    'CLAIM_PAID_DATE': 'object',
                    'COVERED_AMOUNT': 'float64',
                    'PLAN_PAID_AMOUNT': 'category',
                    'PATIENT_SSN': 'category',
                    'INPATIENT_OR_OUTPATIENT': 'category',
                    'CLAIM_CAUSE': 'category',
                    'BENEFIT_CODE': 'category',
                    'NETWORK': 'category',
                    'PROVIDER_NAME': 'category',
                    'PROVIDER_PAID_NAME': 'category',
                    'CHARGED_AMOUNT': 'float64',
                    'UCR': 'category',
                    'CPT_MODIFIER': 'category',
                    'DIAGNOSIS_2': 'category',
                    'DIAGNOSIS_3': 'category',
                    'DIAGNOSIS_4': 'category',
                    'DIAGNOSIS_5': 'category',
                    'MEMBER_DEDUCTIBLE_AMOUNT': 'float64',
                    'MEMBER_OOP_AMOUNT': 'float64',
                    'MEMBER_COPAY_AMOUNT': 'float64',
                    'CLAIM_NUMBER': 'category',
                    'CLAIM_RECEIVED_DATE': 'category',
                    'CLAIM_ENTRY_DATE': 'category',
                    'REMARKS_CODE_1': 'category',
                    'REMARKS_CODE_2': 'category',
                    'REMARKS_CODE_3': 'category',
                    'CHECK_NUMBER': 'category',
                    'BENEFITS_ASSIGNED': 'category',
                    'REVENUE_CODE': 'category',
                    'PROVIDER_EIN': 'category',
                    'PROVIDER_PAID_NPI': 'category',
                    'PROVIDER_PAID_ZIP': 'category',
                    'UNIQUE_PATIENT_ID': 'category',
                    'LOCATION_CODE': 'category',
                    'SUB_GROUP_CODE': 'category',
                    'PLAN_CODE': 'category',
                    'ADMIT_DATE': 'category',
                    'DISCHARGE_DATE': 'category',
                    'ADMISSION_DAYS': 'category',
                    'DISCHARGE_STATUS_CODE': 'category',
                    'POINT_OF_ORIGIN_CODE': 'category',
                    'ADMISSION_DIAGNOSIS_CODE': 'category',
                    'PATIENT_REASON_DIAGNOSIS_CODE': 'category',
                    'CLAIM_FORM_TYPE': 'category',
                    'TYPE_OF_BILL_CODE': 'category',
                    'ORIGINAL_PROCEDURE_CODE': 'category',
                    'ORIGINAL_POS_CODE': 'category',
                    'ORIGINAL_DIAGNOSIS_CODE': 'category',
                    'ORIGINAL_PROVIDER_CODE': 'category'
                },
                assume_missing=True,
                # parse_dates=['SERVICE_START_DATE', 'SERVICE_END_DATE', 'CLAIM_PAID_DATE'],
                # date_format=parse_date,
                blocksize=20e6
            )

            # Getting unique patient ids and provider npi\'s from csv
            members_ids = ddf['UNIQUE_PATIENT_ID'].astype(str).unique().compute().tolist()
            provider_ids = ddf['PROVIDER_NPI'].astype(str).unique().compute().tolist()

            # Getting employer and provider list
            employer_df = get_employer_dataframe(client_id)
            provider_df = get_provider_dataframe()
            employer_id_list = employer_df['employer_id'].tolist()
            provider_number_list = provider_df['provider_number'].astype(int).tolist()
            records = get_all_members_records(client_id, members_ids, True)

            lookup_options = get_lookup_option([SERVICE_TYPE, 12, 13, 14, 16, 20], True)
            diagnostic_code_list = get_diagnostic_code_list()
            provider_code_list = get_provider_code_list_upload(provider_ids)
            procedure_code_list = get_procedure_code_list()
            benefit_code_list = get_benefit_code_list_array()
            place_of_service = get_place_of_service()

            schema = create_schema(user_id, client_id, records, lookup_options, diagnostic_code_list, provider_code_list, procedure_code_list, benefit_code_list, place_of_service)

            print("Converting Dask DataFrame to Pandas DataFrames for validation...")
            # ddf = ddf.map_partitions(preprocess_date_columns, meta=meta)
            partition_lengths = ddf.map_partitions(len).compute()
            start_indices = [sum(partition_lengths[:i]) for i in range(len(partition_lengths))]
            print(start_indices)
            all_errors = []
            generated_records = []

            def process_partition(partition, start_idx):
                validated_records_df, df_errors, error_records_df = validate_chunk(schema, partition, start_idx)
                return {'validated_records_df': validated_records_df, 'df_errors': df_errors, 'error_records_df': error_records_df}

            partitions = ddf.to_delayed()
            futures = [dask.delayed(process_partition)(partition, start_indices[i]) for i, partition in enumerate(partitions)]
            results = dask.compute(*futures)
            all_errors = []
            all_valid_records_df = pd.DataFrame()
            all_error_records_df = pd.DataFrame()

            for result in results:
                all_errors.extend(result['df_errors'])
                all_valid_records_df = pd.concat([all_valid_records_df, result['validated_records_df']], ignore_index=True)
                all_error_records_df = pd.concat([all_error_records_df, result['error_records_df']], ignore_index=True)

            def generate_employer_info():
                all_valid_records_df_unique = all_valid_records_df.drop_duplicates(subset=['EMPLOYER_ID'])
                for _, row in all_valid_records_df_unique.iterrows():

                    res = validate_employer_id(row['EMPLOYER_ID'], user_id, client_id, row)
                    if res['status']:
                        generated_records.append(res['newRecord'])
                #
                # return generated_records
            generate_employer_info()
            print("Processing finished.")

            if all_errors:
                response = {
                    "message": "Validation errors",
                    "errors": all_errors,
                    'error_records': all_error_records_df.to_dict(orient='records'),
                    'generated_records': generated_records,
                    "time_taken": time.time() - start_time
                }
            else:
                response = {
                    "message": "Validation successful",
                    'generated_records': generated_records,
                    "time_taken": time.time() - start_time
                }

        except Exception as e:
            response = {
                "message": "Error processing file",
                "error": str(e),
                "time_taken": time.time() - start_time
            }

        return jsonify(response), 200

    return app


if __name__ == '__main__':
    import multiprocessing
    multiprocessing.freeze_support()
    app = create_app()
    app.run(debug=True)
