import pandas as pd
from sqlalchemy import create_engine
from constants import connection_string

engine = create_engine(connection_string)


def get_lookup_option(lookup_ids, to_lower=False):
    if not hasattr(get_lookup_option, 'lookup_options'):
        options = pd.read_sql_query(
            "SELECT ln.type_id, lo.option_id, lo.option_value FROM lookup_name ln "
            "INNER JOIN tbl_sir_lookup_options lo ON ln.lookup_id = lo.lookup_id "
            "WHERE ln.type_id IN %(lookup_ids)s AND lo.option_status = 1",
            con=engine,
            params={'lookup_ids': tuple(lookup_ids)}
        )
        final_options = {}
        for _, option in options.iterrows():
            data = option.groupby('type_id')['option_value'].apply(list).to_dict()
            if to_lower:
                data = {k: list(map(str.lower, v)) for k, v in data.items()}
            final_options.update(data)
        get_lookup_option.lookup_options = final_options
    return get_lookup_option.lookup_options


def get_diagnostic_code_list(is_reverse=False):
    if not hasattr(get_diagnostic_code_list, 'diagnostic_codes'):
        condition = "" if not get_diagnostic_code_list.get_default else f"WHERE diagnosis_code = {get_diagnostic_code_list.default_diagnostic_code}"
        data = pd.read_sql_query(
            f"SELECT condition_id, diagnosis_code FROM tbl_ph_conditions {condition}",
            con=engine
        )
        if is_reverse:
            diagnostic_codes = dict(zip(data['condition_id'], data['diagnosis_code']))
        else:
            diagnostic_codes = dict(zip(data['diagnosis_code'], data['condition_id']))
        get_diagnostic_code_list.diagnostic_codes = diagnostic_codes
    return get_diagnostic_code_list.diagnostic_codes


def get_provider_code_list_upload(provider_ids, is_reverse=False):
    if not hasattr(get_provider_code_list_upload, 'provider_codes'):
        condition = ""
        if provider_ids:
            providers = '", "'.join(provider_ids)
            if is_reverse:
                condition = f"WHERE provider_id IN ('{providers}')"
            else:
                condition = f"WHERE provider_number IN ('{providers}')"
        data = pd.read_sql_query(
            f"SELECT provider_id, provider_number FROM tbl_ph_providers {condition}",
            con=engine
        )
        if is_reverse:
            provider_codes = dict(zip(data['provider_number'], data['provider_id']))
        else:
            provider_codes = dict(zip(data['provider_id'], data['provider_number']))
        get_provider_code_list_upload.provider_codes = provider_codes
    return get_provider_code_list_upload.provider_codes


def get_procedure_code_list(is_reverse=False):
    if not hasattr(get_procedure_code_list, 'procedure_codes'):
        condition = "" if not get_procedure_code_list.get_default else f"WHERE procedure_code = {get_procedure_code_list.default_procedure_code}"
        data = pd.read_sql_query(
            f"SELECT procedure_id, procedure_code FROM tbl_ph_procedures {condition}",
            con=engine
        )
        if is_reverse:
            procedure_codes = dict(zip(data['procedure_code'], data['procedure_id']))
        else:
            procedure_codes = dict(zip(data['procedure_id'], data['procedure_code']))
        get_procedure_code_list.procedure_codes = procedure_codes
    return get_procedure_code_list.procedure_codes


def get_benefit_code_list_array():
    if not hasattr(get_benefit_code_list_array, 'benefit_codes'):
        data = pd.read_sql_query(
            "SELECT benefit_id, benefit_code FROM tbl_ph_benefit_code",
            con=engine
        )
        benefit_codes = dict(zip(data['benefit_code'], data['benefit_id']))
        get_benefit_code_list_array.benefit_codes = benefit_codes
    return get_benefit_code_list_array.benefit_codes
