import pdb
import re
import pandas as pd
import pandera as pa
from flask import g
from pandera import DataFrameSchema, Column, Check
from constants import CLAIM_STATUS, CLAIM_TYPE, INPATIENT_OR_OUTPATIENT, CLAIM_CAUSE, BENEFIT_ASSIGNED, CLAIM_FORM_TYPE
from get_info import get_employer_dataframe, get_provider_dataframe
from validate_employer_id import employer_id_check

basic_date_pattern = re.compile(r'^(0[1-9]|1[0-2])[-/](0[1-9]|[12][0-9]|3[01])[-/](\d{4})$')


def is_valid_date(date_str):
    if not basic_date_pattern.match(date_str):
        return False
    try:
        month, day, year = map(int, re.split('[-/]', date_str))
        if month == 2:
            # Check for leap year
            if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
                return 1 <= day <= 29
            else:
                return 1 <= day <= 28
        elif month in {4, 6, 9, 11}:
            return 1 <= day <= 30
        else:
            return 1 <= day <= 31
    except ValueError:
        return False


def check_date_format(date_series):
    return date_series.apply(lambda x: is_valid_date(x) if pd.notnull(x) else True)


def create_schema(user_id, client_id, records, lookup_options, diagnostic_code_list, provider_code_list, procedure_code_list, benefit_code_list, place_of_service):
    # print(provider_code_list)
    # print("---------")
    # print(procedure_code_list)
    # print("------------")
    # print(place_of_service)
    # pdb.set_trace()
    # def employer_id_row_check(row):
    #     employer_id = row['EMPLOYER_ID']
    #     return employer_id_check(user_id, client_id)(employer_id, row)

    schema = DataFrameSchema(
        {
            "EMPLOYER_ID": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=False),
            "EMPLOYER_NAME": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_ ]+$')], nullable=False),
            "CLAIM_STATUS": Column(pa.String, checks=[pa.Check.isin([claim_status for claim_status, _ in CLAIM_STATUS])], nullable=True),
            "CLAIM_TYPE": Column(pa.String, checks=[pa.Check.isin(list(CLAIM_TYPE.keys()))], nullable=False),
            "SERVICE_START_DATE": Column(pa.String, checks=[pa.Check(check_date_format, element_wise=False)], nullable=True),
            "SERVICE_END_DATE": Column(pa.String, checks=[pa.Check(check_date_format, element_wise=False)], nullable=True),
            "PROVIDER_NPI": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$'), pa.Check.isin(list(provider_code_list.values()))], nullable=True),
            # "PROVIDER_NPI": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "PLACE_OF_SERVICE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$'), pa.Check.isin(list(place_of_service.keys()))], nullable=True),
            # "PLACE_OF_SERVICE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "CPT_PROCEDURE": Column(pa.String, checks=[pa.Check.isin(list(procedure_code_list.values()))], nullable=True),
            # "CPT_PROCEDURE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "DIAGNOSIS_1": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "CLAIM_PAID_DATE": Column(pa.String, checks=[pa.Check(check_date_format, element_wise=False)], nullable=False),
            "COVERED_AMOUNT": Column(pa.String, checks=[pa.Check.str_matches(r'^(\d+(\.\d*)?|\.\d+)$')], nullable=True),
            "PLAN_PAID_AMOUNT": Column(pa.String, checks=[pa.Check.str_matches(r'^(\d+(\.\d*)?|\.\d+)$')], nullable=False),
            "PATIENT_SSN": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "INPATIENT_OR_OUTPATIENT": Column(pa.String, checks=[pa.Check.isin(list(INPATIENT_OR_OUTPATIENT.keys()))], nullable=True),
            "CLAIM_CAUSE": Column(pa.String, checks=[pa.Check.isin(list(CLAIM_CAUSE.keys()))], nullable=True),
            "BENEFIT_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "NETWORK": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "PROVIDER_NAME": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_ ]+$')], nullable=True),
            "PROVIDER_PAID_NAME": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "CHARGED_AMOUNT": Column(pa.String, checks=[pa.Check.str_matches(r'^(\d+(\.\d*)?|\.\d+)$')], nullable=True),
            "UCR": Column(pa.String, checks=[pa.Check.str_matches(r'^(\d+(\.\d*)?|\.\d+)$')], nullable=True),
            "CPT_MODIFIER": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "DIAGNOSIS_2": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "DIAGNOSIS_3": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "DIAGNOSIS_4": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "DIAGNOSIS_5": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "MEMBER_DEDUCTIBLE_AMOUNT": Column(pa.String, checks=[pa.Check.str_matches(r'^(\d+(\.\d*)?|\.\d+)$')], nullable=True),
            "MEMBER_OOP_AMOUNT": Column(pa.String, checks=[pa.Check.str_matches(r'^(\d+(\.\d*)?|\.\d+)$')], nullable=True),
            "MEMBER_COPAY_AMOUNT": Column(pa.String, checks=[pa.Check.str_matches(r'^(\d+(\.\d*)?|\.\d+)$')], nullable=True),
            "CLAIM_NUMBER": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "CLAIM_RECEIVED_DATE": Column(pa.String, checks=[pa.Check(check_date_format, element_wise=False)], nullable=True),
            "CLAIM_ENTRY_DATE": Column(pa.String, checks=[pa.Check(check_date_format, element_wise=False)], nullable=True),
            "REMARKS_CODE_1": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "REMARKS_CODE_2": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "REMARKS_CODE_3": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "CHECK_NUMBER": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "BENEFITS_ASSIGNED": Column(pa.String, checks=[pa.Check.isin(list(BENEFIT_ASSIGNED.keys()))], nullable=True),
            "REVENUE_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "PROVIDER_EIN": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "PROVIDER_PAID_NPI": Column(pa.String, checks=[pa.Check.isin(list(provider_code_list.values()))], nullable=True),
            # "PROVIDER_PAID_NPI": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "PROVIDER_PAID_ZIP": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "UNIQUE_PATIENT_ID": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_-]+$')], nullable=False),
            "LOCATION_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "SUB_GROUP_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "PLAN_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "ADMIT_DATE": Column(pa.String, checks=[pa.Check(check_date_format, element_wise=False)], nullable=True),
            "DISCHARGE_DATE": Column(pa.String, checks=[pa.Check(check_date_format, element_wise=False)], nullable=True),
            "ADMISSION_DAYS": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "DISCHARGE_STATUS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "POINT_OF_ORIGIN_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "ADMISSION_DIAGNOSIS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "PATIENT_REASON_DIAGNOSIS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
            "CLAIM_FORM_TYPE": Column(pa.String, checks=[pa.Check.isin(list(CLAIM_FORM_TYPE.keys()))], required=False, nullable=True),
            "TYPE_OF_BILL_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], required=False,  nullable=True),
            "ORIGINAL_PROCEDURE_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], required=False, nullable=True),
            "ORIGINAL_POS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], required=False, nullable=True),
            "ORIGINAL_DIAGNOSIS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], required=False, nullable=True),
            "ORIGINAL_PROVIDER_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], required=False, nullable=True),
        }
    )
    return schema
