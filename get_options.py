import pdb

import pandas as pd
from sqlalchemy import create_engine
from constants import connection_string

engine = create_engine(connection_string)


def get_lookup_option(lookup_ids, to_lower=False):
    if not hasattr(get_lookup_option, 'lookup_options'):
        get_lookup_option.lookup_options = {}

    if not get_lookup_option.lookup_options:
        try:
            # Prepare the query to get the lookup options
            query = """
                SELECT ln.type_id, lo.option_id, lo.option_value 
                FROM tbl_sir_lookup_name ln 
                INNER JOIN tbl_sir_lookup_options lo ON ln.type_id = lo.option_type 
                WHERE ln.type_id IN %(lookup_ids)s AND lo.option_status = 1
            """

            # Execute the query
            options = pd.read_sql_query(query, con=engine, params={'lookup_ids': tuple(lookup_ids)})

            # Group by type_id and map option_id to option_value
            final_options = {}
            for type_id, group in options.groupby('type_id'):
                data = group.set_index('option_id')['option_value'].to_dict()
                if to_lower:
                    data = {k: v.lower() for k, v in data.items()}
                final_options[type_id] = data

            get_lookup_option.lookup_options = final_options

        except Exception as e:
            print(f"An error occurred: {e}")

    return get_lookup_option.lookup_options


def get_diagnostic_code_list(is_reverse=False):

    if not hasattr(get_diagnostic_code_list, 'diagnostic_codes'):
        get_diagnostic_code_list.diagnostic_codes = None
    if not hasattr(get_diagnostic_code_list, 'get_default'):
        get_diagnostic_code_list.get_default = False
    if not hasattr(get_diagnostic_code_list, 'default_diagnostic_code'):
        get_diagnostic_code_list.default_diagnostic_code = None

    if get_diagnostic_code_list.diagnostic_codes is not None:
        return get_diagnostic_code_list.diagnostic_codes

    condition = ""
    if get_diagnostic_code_list.get_default:
        condition = f"WHERE diagnosis_code = {get_diagnostic_code_list.default_diagnostic_code}"

    # Execute the SQL query
    query = f"SELECT condition_id, diagnosis_code FROM tbl_ph_conditions {condition}"
    data = pd.read_sql_query(query, con=engine)

    if not data.empty:
        if is_reverse:
            diagnostic_codes = dict(zip(data['condition_id'], data['diagnosis_code']))
        else:
            diagnostic_codes = dict(zip(data['diagnosis_code'], data['condition_id']))
        get_diagnostic_code_list.diagnostic_codes = diagnostic_codes
    else:
        get_diagnostic_code_list.diagnostic_codes = {}

    return get_diagnostic_code_list.diagnostic_codes


# get_diagnostic_code_list.get_default = True
# get_diagnostic_code_list.default_diagnostic_code = '00000000'

def get_provider_code_list_upload(provider_ids, is_reverse=False):

    if not hasattr(get_provider_code_list_upload, 'provider_codes'):
        get_provider_code_list_upload.provider_codes = None

    if get_provider_code_list_upload.provider_codes is not None:
        return get_provider_code_list_upload.provider_codes

    condition = ""
    if provider_ids:

        providers = '", "'.join(provider_ids)
        if is_reverse:
            condition = f'WHERE provider_id IN ("{providers}")'
        else:
            condition = f'WHERE provider_number IN ("{providers}")'

    query = f"SELECT provider_id, provider_number FROM tbl_ph_providers {condition}"
    data = pd.read_sql_query(query, con=engine)

    if not data.empty:
        if is_reverse:
            provider_codes = dict(zip(data['provider_number'], data['provider_id']))
        else:
            provider_codes = dict(zip(data['provider_id'], data['provider_number']))

        get_provider_code_list_upload.provider_codes = provider_codes
    else:
        get_provider_code_list_upload.provider_codes = {}

    return get_provider_code_list_upload.provider_codes


def get_procedure_code_list(is_reverse=False):

    if not hasattr(get_procedure_code_list, 'procedure_codes'):
        get_procedure_code_list.procedure_codes = None
    if not hasattr(get_procedure_code_list, 'get_default'):
        get_procedure_code_list.get_default = False
    if not hasattr(get_procedure_code_list, 'default_procedure_code'):
        get_procedure_code_list.default_procedure_code = None

    if get_procedure_code_list.procedure_codes is not None:
        return get_procedure_code_list.procedure_codes

    condition = ""
    if get_procedure_code_list.get_default and get_procedure_code_list.default_procedure_code is not None:
        condition = f"WHERE procedure_code = '{get_procedure_code_list.default_procedure_code}'"

    query = f"SELECT procedure_id, procedure_code FROM tbl_ph_procedures {condition}"
    data = pd.read_sql_query(query, con=engine)

    if not data.empty:
        if is_reverse:
            procedure_codes = dict(zip(data['procedure_code'], data['procedure_id']))
        else:
            procedure_codes = dict(zip(data['procedure_id'], data['procedure_code']))
        get_procedure_code_list.procedure_codes = procedure_codes
    else:
        get_procedure_code_list.procedure_codes = {}

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
