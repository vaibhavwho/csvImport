import pdb
import re

import pandas as pd
import pandera as pa
from pandera import DataFrameSchema, Column, Check
from constants import CLAIM_STATUS, CLAIM_TYPE, INPATIENT_OR_OUTPATIENT

# # For float values validation

# check_positive_float = Check(lambda x: re.match(r'^[a-zA-Z0-9_ ]*$', str(x)) if not float(x) else x > 0)
# check_positive_float = Check(lambda x: bool(re.match(r'^\d*\.?\d+$', str(x))), error="Non-numerical value found")

# # For validation of date format

# def date_format_parser(series: pd.Series) -> pd.Series:
#     def parse_date(date_string):
#         if isinstance(date_string, str):
#             if '/' in date_string:
#                 return pd.to_datetime(date_string, format='%m/%d/%Y', errors='coerce')
#             elif '-' in date_string:
#                 date_string = date_string.replace("-", "/")
#                 return pd.to_datetime(date_string, format='%m/%d/%Y', errors='coerce')
#         return pd.NaT
#
#     parsed_dates = series.apply(parse_date)
#     return ~parsed_dates.isna()


# def parse_date(date_series):
#     def try_parsing_date(date_string):
#         for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%d-%b-%y'):
#             try:
#                 return pd.to_datetime(date_string, format=fmt, errors='coerce')
#             except ValueError:
#                 pass
#         return pd.NaT
#
#     return date_series.apply(try_parsing_date)

# def valid_date_format(date_string):
#     pattern = r'^\d{2}/\d{2}/\d{4}$|^\d{4}-\d{2}-\d{2}$|^\d{2}-[a-zA-Z]{3}-\d{2}$'
#     return bool(re.match(pattern, date_string))

# date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$|^\d{2}-\d{2}-\d{4}$')
# def check_date_format(date_series):
#     return date_series.apply(lambda x: bool(date_pattern.match(x)) if pd.notnull(x) else True)
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


# Define the schema
schema = DataFrameSchema(
    {
        "EMPLOYER_ID": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=False),
        "EMPLOYER_NAME": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=False),
        "CLAIM_STATUS": Column(pa.String, checks=[pa.Check.isin([claim_status for claim_status, _ in CLAIM_STATUS])], nullable=True),
        "CLAIM_TYPE": Column(pa.String, checks=[pa.Check.isin(list(CLAIM_TYPE.keys()))], nullable=False),
        "SERVICE_START_DATE": Column(pa.String, checks=[pa.Check(check_date_format, element_wise=False)], nullable=True),
        "SERVICE_END_DATE": Column(pa.String, checks=[pa.Check(check_date_format, element_wise=False)], nullable=True),
        "PROVIDER_NPI": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PLACE_OF_SERVICE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CPT_PROCEDURE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DIAGNOSIS_1": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CLAIM_PAID_DATE": Column(pa.String, checks=[pa.Check(check_date_format, element_wise=False)], nullable=True),
        "COVERED_AMOUNT": Column(pa.Float, checks=[pa.Check(lambda x: x >= 0)], nullable=True),
        "PLAN_PAID_AMOUNT": Column(pa.String, checks=[pa.Check.str_matches(r'^(\d+(\.\d*)?|\.\d+)$')], nullable=True),
        "PATIENT_SSN": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "INPATIENT_OR_OUTPATIENT": Column(pa.String, checks=[pa.Check.isin(list(INPATIENT_OR_OUTPATIENT.keys()))], nullable=True),
        "CLAIM_CAUSE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "BENEFIT_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "NETWORK": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PROVIDER_NAME": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PROVIDER_PAID_NAME": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CHARGED_AMOUNT": Column(pa.Float, checks=[pa.Check(lambda x: x >= 0)], nullable=True),
        "UCR": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CPT_MODIFIER": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DIAGNOSIS_2": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DIAGNOSIS_3": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DIAGNOSIS_4": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DIAGNOSIS_5": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "MEMBER_DEDUCTIBLE_AMOUNT": Column(pa.Float, checks=[pa.Check(lambda x: x >= 0)], nullable=True),
        "MEMBER_OOP_AMOUNT": Column(pa.Float, checks=[pa.Check(lambda x: x >= 0)], nullable=True),
        "MEMBER_COPAY_AMOUNT": Column(pa.Float, checks=[pa.Check(lambda x: x >= 0)], nullable=True),
        "CLAIM_NUMBER": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CLAIM_RECEIVED_DATE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CLAIM_ENTRY_DATE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "REMARKS_CODE_1": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "REMARKS_CODE_2": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "REMARKS_CODE_3": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CHECK_NUMBER": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "BENEFITS_ASSIGNED": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "REVENUE_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PROVIDER_EIN": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PROVIDER_PAID_NPI": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PROVIDER_PAID_ZIP": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "UNIQUE_PATIENT_ID": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_-]+$')], nullable=False),
        "LOCATION_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "SUB_GROUP_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PLAN_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "ADMIT_DATE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DISCHARGE_DATE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "ADMISSION_DAYS": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "DISCHARGE_STATUS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "POINT_OF_ORIGIN_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "ADMISSION_DIAGNOSIS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "PATIENT_REASON_DIAGNOSIS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], nullable=True),
        "CLAIM_FORM_TYPE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], required=False, nullable=True),
        "TYPE_OF_BILL_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], required=False,  nullable=True),
        "ORIGINAL_PROCEDURE_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], required=False, nullable=True),
        "ORIGINAL_POS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], required=False, nullable=True),
        "ORIGINAL_DIAGNOSIS_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], required=False, nullable=True),
        "ORIGINAL_PROVIDER_CODE": Column(pa.String, checks=[pa.Check.str_matches(r'^[a-zA-Z0-9_]+$')], required=False, nullable=True),
    }
)
