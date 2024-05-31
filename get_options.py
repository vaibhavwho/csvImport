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

            options = pd.read_sql_query(query, con=engine, params={'lookup_ids': tuple(lookup_ids)})

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


def get_diagnostic_code_list(diagnostic_codes, is_reverse=False):

    diagnostic_codes = tuple(diagnostic_codes)

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
    else:
        formatted_providers = ", ".join(f"'{dcode}'" for dcode in diagnostic_codes)
        condition = f'WHERE diagnosis_code IN ({formatted_providers})'

    # Execute the SQL query
    query = f"SELECT condition_id, diagnosis_code FROM tbl_ph_conditions {condition}"
    data = pd.read_sql_query(query, con=engine)
    data['diagnosis_code'] = data['diagnosis_code'].str.strip()
    data = data[data['diagnosis_code'] != '']
    data = data.dropna(subset='diagnosis_code')
    if not data.empty:
        if not is_reverse:
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
    provider_ids = tuple(provider_ids)
    if not hasattr(get_provider_code_list_upload, 'provider_codes'):
        get_provider_code_list_upload.provider_codes = None

    if get_provider_code_list_upload.provider_codes is not None:
        return get_provider_code_list_upload.provider_codes

    condition = ""
    if provider_ids:
        providers = '", "'.join(provider_ids)
        if is_reverse:
            formatted_providers = ", ".join(f"'{prov}'" for prov in provider_ids)
            condition = f'WHERE provider_id IN ("{formatted_providers}")'
        else:
            formatted_providers = ", ".join(f"'{prov}'" for prov in provider_ids)
            condition = f" WHERE provider_number IN ({formatted_providers})"


    # print(f"SQL Query Condition: {condition}")
    query = f"SELECT provider_id, provider_number FROM tbl_ph_providers {condition}"
    data = pd.read_sql_query(query, con=engine)
    data = data.dropna(subset='provider_number')

    if not data.empty:
        if is_reverse:
            provider_codes = dict(zip(data['provider_number'], data['provider_id']))
        else:
            provider_codes = dict(zip(data['provider_id'], data['provider_number']))

        get_provider_code_list_upload.provider_codes = provider_codes
    else:
        get_provider_code_list_upload.provider_codes = {}

    return get_provider_code_list_upload.provider_codes


def get_procedure_code_list(cpt_procedures, is_reverse=False):
    cpt_procedures = tuple(cpt_procedures)
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

    query = f"SELECT procedure_id, procedure_code FROM tbl_ph_procedures"

    if condition:
        query += " {condition} AND "
    elif cpt_procedures:
        formatted_procedures = ", ".join(f"'{proc}'" for proc in cpt_procedures)
        query += f" WHERE procedure_code IN ({formatted_procedures})"
    data = pd.read_sql_query(query, con=engine)
    data['procedure_code'] = data['procedure_code'].str.strip()
    data = data[data['procedure_code'] != '']
    data = data.dropna(subset='procedure_code')
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

def get_place_of_service(is_reverse=False):
    get_place_of_service.default_pos_code = '0000'
    if not hasattr(get_place_of_service, 'place_of_service'):
        condition = ""
        if getattr(get_place_of_service, 'get_default', False):
            condition = f"WHERE place_of_service_code = {get_place_of_service.default_pos_code}"

        sql = f"SELECT place_of_service_id, place_of_service_code, place_of_service_name FROM tbl_ph_place_of_services {condition}"
        data = pd.read_sql_query(sql, con=engine)

        if not data.empty:
            if not is_reverse:
                place_of_service = dict(zip(data['place_of_service_id'], data['place_of_service_code']))
            else:
                place_of_service = dict(zip(data['place_of_service_code'], data['place_of_service_id']))
                place_of_service_name = dict(zip(data['place_of_service_id'], data['place_of_service_name']))

                get_place_of_service.place_of_service_name = place_of_service_name

            get_place_of_service.place_of_service = place_of_service

    return get_place_of_service.place_of_service
