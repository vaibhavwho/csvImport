import os
import pdb
import re
import time
from flask import Flask, request, jsonify
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
from schema import schema
from get_all_member_records import get_all_members_records
from get_info import get_employer_dataframe, get_provider_dataframe
from get_options import get_lookup_option, get_diagnostic_code_list, get_provider_code_list_upload, \
    get_procedure_code_list, get_benefit_code_list_array


def validate_chunk(chunk, start_index):
    print(f"Process {os.getpid()} started for chunk.")
    errors = []
    try:
        schema.validate(chunk, lazy=True)
    except (pa.errors.SchemaErrors, ValueError) as e:
        for error in e.failure_cases.to_dict(orient='records'):
            error_index = error['index']
            if error_index is not None:
                row_number = start_index + error_index
            else:
                row_number = None
            errors.append({"index": error_index, "column": error['column'], "error": error['failure_case'], "row_number": row_number})
            # break
    print(f"Process {os.getpid()} finished for chunk.")
    return errors


def create_app():
    app = Flask(__name__)

    @app.route('/process_file', methods=['POST'])
    def validate_csv():
        start_time = time.time()
        request_data = request.json
        file_path = request_data.get('file_path')
        client_id = request_data.get('client_id')
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
            # members_ids = ddf['UNIQUE_PATIENT_ID'].astype(str).unique().compute().tolist()
            # provider_ids = ddf['PROVIDER_NPI'].astype(str).unique().compute().tolist()
            # get_lookup_option([SERVICE_TYPE, 12, 13, 14, 16, 20], True)
            # get_diagnostic_code_list()
            # get_provider_code_list_upload(provider_ids)
            # get_procedure_code_list()
            # get_benefit_code_list_array()
            # employer_df = get_employer_dataframe(client_id)
            # get_all_members_records(client_id, members_ids, True)

            print("Converting Dask DataFrame to Pandas DataFrames for validation...")

            # ddf = ddf.map_partitions(preprocess_date_columns, meta=meta)
            partition_lengths = ddf.map_partitions(len).compute()
            start_indices = [sum(partition_lengths[:i]) for i in range(len(partition_lengths))]
            print(start_indices)
            all_errors = []

            def process_partition(partition, start_idx):
                df_errors = validate_chunk(partition, start_idx)
                return df_errors
            partitions = ddf.to_delayed()
            futures = [dask.delayed(process_partition)(partition, start_indices[i]) for i, partition in enumerate(partitions)]
            results = dask.compute(*futures)

            for result in results:
                all_errors.extend(result)

            print("Processing finished.")

            if all_errors:
                response = {
                    "message": "Validation errors",
                    "errors": all_errors,
                    "time_taken": time.time() - start_time
                }
            else:
                response = {
                    "message": "Validation successful",
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
