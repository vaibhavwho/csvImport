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
from insert_data_into_db import insert_data, engine, metadata
from validate_employer_id import validate_employer_id
from concurrent.futures import ThreadPoolExecutor, as_completed


def validate_chunk(schema, chunk, start_index):
    print(f"Process {os.getpid()} started for chunk.")
    errors = []
    validated_records_df = pd.DataFrame()
    error_records_df = pd.DataFrame()
    unique_error_rows = set()

    try:
        validated_chunk = schema.validate(chunk, lazy=True)
        validated_records_df = pd.concat([validated_records_df, validated_chunk], ignore_index=True)
    except pa.errors.SchemaErrors as e:
        for error in e.failure_cases.to_dict(orient='records'):
            error_index = error['index']
            row_number = start_index + error_index if error_index is not None else None
            unique_error_rows.add(row_number)
            errors.append({
                "index": error_index,
                "column": error['column'],
                "error": error['failure_case'],
                "row_number": row_number
            })
            if len(unique_error_rows) >= 1000:
                print("Error row limit reached, breaking validation process.")
                break

        erroneous_indices = e.failure_cases['index'].tolist()
        error_records_df = pd.concat([error_records_df, chunk.loc[erroneous_indices]], ignore_index=True)
        valid_chunk = chunk.drop(index=erroneous_indices)
        validated_records_df = pd.concat([validated_records_df, valid_chunk], ignore_index=True)

    print(f"Process {os.getpid()} finished for chunk.")
    return validated_records_df, errors, error_records_df, len(unique_error_rows)


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
                    'COVERED_AMOUNT': 'category',
                    'PLAN_PAID_AMOUNT': 'category',
                    'PATIENT_SSN': 'category',
                    'INPATIENT_OR_OUTPATIENT': 'category',
                    'CLAIM_CAUSE': 'category',
                    'BENEFIT_CODE': 'category',
                    'NETWORK': 'category',
                    'PROVIDER_NAME': 'category',
                    'PROVIDER_PAID_NAME': 'category',
                    'CHARGED_AMOUNT': 'category',
                    'UCR': 'category',
                    'CPT_MODIFIER': 'category',
                    'DIAGNOSIS_2': 'category',
                    'DIAGNOSIS_3': 'category',
                    'DIAGNOSIS_4': 'category',
                    'DIAGNOSIS_5': 'category',
                    'MEMBER_DEDUCTIBLE_AMOUNT': 'category',
                    'MEMBER_OOP_AMOUNT': 'category',
                    'MEMBER_COPAY_AMOUNT': 'category',
                    'CLAIM_NUMBER': 'category',
                    'CLAIM_RECEIVED_DATE': 'object',
                    'CLAIM_ENTRY_DATE': 'object',
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
                    'ADMIT_DATE': 'object',
                    'DISCHARGE_DATE': 'object',
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

            members_ids = ddf['UNIQUE_PATIENT_ID'].astype(str).unique().compute().tolist()
            provider_ids = ddf['PROVIDER_NPI'].astype(str).unique().compute().tolist()

            employer_df = get_employer_dataframe(client_id)
            provider_df = get_provider_dataframe()
            employer_id_list = employer_df['employer_id'].tolist()
            provider_number_list = provider_df['provider_number'].astype(int).tolist()

            # start_seq_time = time.time()
            # records = get_all_members_records(client_id, members_ids, True)
            # lookup_options = get_lookup_option([SERVICE_TYPE, 12, 13, 14, 16, 20], True)
            # diagnostic_code_list = get_diagnostic_code_list()
            # provider_code_list = get_provider_code_list_upload(provider_ids, False)
            # procedure_code_list = get_procedure_code_list()
            # benefit_code_list = get_benefit_code_list_array()
            # place_of_service = get_place_of_service()
            # end_seq_time = time.time()
            # print(f"Sequential fetching took {end_seq_time - start_seq_time} seconds")
            # schema = create_schema(
            #     user_id,
            #     client_id,
            #     records,
            #     lookup_options,
            #     diagnostic_code_list,
            #     provider_code_list,
            #     procedure_code_list,
            #     benefit_code_list,
            #     place_of_service
            # )
            with ThreadPoolExecutor() as executor:
                start_parallel_time = time.time()
                futures = {
                    executor.submit(get_all_members_records, client_id, members_ids, True): "records",
                    executor.submit(get_lookup_option, [SERVICE_TYPE, 12, 13, 14, 16, 20], True): "lookup_options",
                    executor.submit(get_diagnostic_code_list): "diagnostic_code_list",
                    executor.submit(get_provider_code_list_upload, provider_ids, False): "provider_code_list",
                    executor.submit(get_procedure_code_list): "procedure_code_list",
                    executor.submit(get_benefit_code_list_array): "benefit_code_list",
                    executor.submit(get_place_of_service): "place_of_service"
                }

                results = {}
                for future in as_completed(futures):
                    results[futures[future]] = future.result()
                end_parallel_time = time.time()
                print(f"Parallel fetching took {end_parallel_time - start_parallel_time} seconds")

            schema = create_schema(
                user_id,
                client_id,
                results["records"],
                results["lookup_options"],
                results["diagnostic_code_list"],
                results["provider_code_list"],
                results["procedure_code_list"],
                results["benefit_code_list"],
                results["place_of_service"]
            )

            partition_lengths = ddf.map_partitions(len).compute()
            start_indices = [sum(partition_lengths[:i]) for i in range(len(partition_lengths))]
            print(start_indices)

            all_errors = []
            generated_records = []
            error_file_path = f"validation_errors_{time.strftime('%d-%b-%Y_%a-%H:%M:%S')}.csv"

            def process_partition(partition, start_idx):
                validated_records_df, df_errors, error_records_df, unique_error_row_count = validate_chunk(schema,
                                                                                                           partition,
                                                                                                           start_idx)
                return {'validated_records_df': validated_records_df, 'df_errors': df_errors,
                        'error_records_df': error_records_df, 'unique_error_row_count': unique_error_row_count}

            partitions = ddf.to_delayed()
            futures = [dask.delayed(process_partition)(partition, start_indices[i]) for i, partition in
                       enumerate(partitions)]
            results = dask.compute(*futures)

            unique_error_row_count = 0
            all_valid_records_df = pd.DataFrame()
            all_error_records_df = pd.DataFrame()

            for result in results:
                all_errors.extend(result['df_errors'])
                all_valid_records_df = pd.concat([all_valid_records_df, result['validated_records_df']],
                                                 ignore_index=True)
                all_error_records_df = pd.concat([all_error_records_df, result['error_records_df']], ignore_index=True)
                unique_error_row_count += result['unique_error_row_count']
                if unique_error_row_count >= 1000:
                    print("Total error row limit reached, saving errors to CSV.")
                    all_error_records_df.head(1000).to_csv(error_file_path, index=False)
                    response = {
                        "message": "Validation errors exceeded the limit",
                        "errors": all_errors[:1000],
                        "error_file_path": error_file_path,
                        "time_taken": time.time() - start_time
                    }
                    return jsonify(response), 200

            def generate_employer_info():
                all_valid_records_df_unique = all_valid_records_df.drop_duplicates(subset=['EMPLOYER_ID'])
                for _, row in all_valid_records_df_unique.iterrows():
                    res = validate_employer_id(row['EMPLOYER_ID'], user_id, client_id, row)
                    if res['status']:
                        generated_records.append(res['newRecord'])

            generate_employer_info()

            print("Processing finished.")
            print("Inserting Data in Database")
            insert_data(all_valid_records_df, generated_records, engine, metadata, client_id, user_id)
            print("Insertion Finished")
            if all_errors:
                response = {
                    "message": "Validation errors",
                    "errors": all_errors,
                    # 'error_records': all_error_records_df.to_dict(orient='records'),
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
