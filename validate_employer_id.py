import pdb

import pandas as pd
import pandera as pa
from datetime import datetime, timedelta
import re


def set_default_attributes(user_id, record):
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    record['created_at'] = current_time
    record['created_by'] = user_id if user_id else None
    record['updated_at'] = current_time
    record['updated_by'] = user_id if user_id else None
    return record


def prepare_employer_information(employer_id, user_id, client_id, employer_name):
    record = {
        "id": '',
        "client_id": client_id,
        "employer_id": employer_id,
        "employer_name": employer_name
    }
    record = set_default_attributes(user_id, record)
    return record


def validate_member_number(member_id):
    member_id = member_id.strip()
    if not member_id:
        return {"status": False, "errorMessage": "Employer ID cannot be empty."}
    if len(member_id) > 20:
        return {"status": False, "errorMessage": "Employer ID should be maximum 20 chars length."}
    if not re.match(r"^[a-zA-Z0-9&._#$-']*$", member_id):
        return {"status": False, "errorMessage": "Special characters are not allowed other than &,._#$-'"}
    return {"status": True, "value": member_id}


def validate_employer_id(employer_id, user_id, client_id, row):
    return_dict = {"status": False}

    val_status = validate_member_number(employer_id)
    if not val_status['status']:
        val_status['field'] = 'Employer Id'
        return val_status

    employer_name_value = row.get('EMPLOYER_NAME', '').strip()
    if not employer_name_value:
        return {"status": False, "field": 'Employer Name', "errorMessage": "Employer Name cannot be empty."}
    if len(employer_name_value) > 40:
        return {"status": False, "field": 'Employer Name', "errorMessage": "Employer Name should be maximum 40 chars length."}
    if not re.match(r"^[a-zA-Z0-9 ]*$", employer_name_value):
        return {"status": False, "field": 'Employer Name', "errorMessage": "Employer Name contains invalid characters."}

    return_dict['status'] = True
    return_dict['duplicateStatus'] = False
    return_dict['value'] = employer_id
    return_dict['newRecord'] = prepare_employer_information(employer_id, user_id, client_id, employer_name_value)
    return return_dict


def employer_id_check(user_id, client_id):
    def _check(employer_id, row):
        result = validate_employer_id(employer_id, user_id, client_id, row)
        if not result['status']:
            raise ValueError(result['errorMessage'])
        # Optionally store or use result['newRecord'] here if needed
        return True
    return _check

# def validate_non_utf(value):
#     try:
#         value.encode('utf-8')
#         return value
#     except UnicodeEncodeError:
#         return value.encode('utf-8', 'ignore').decode('utf-8')
#
#
# def validate_pregmatch(value):
#     # Adjust the regex pattern according to your requirements
#     pattern = r'^[a-zA-Z0-9&._#$\'-]+$'
#     return bool(re.match(pattern, value))
#
#
# def validate_member_number(member_id):
#     member_id = validate_non_utf(member_id)
#     member_id = member_id.strip()
#     if not member_id:
#         return False, 'Employer ID cannot be empty.'
#     if len(member_id) > 20:
#         return False, 'Employer ID should be maximum 20 chars length.'
#     if not validate_pregmatch(member_id):
#         return False, 'Special characters are not allowed other than &,._#$-\''
#     return True, member_id
#
#
# def set_default_attributes(user_id, record):
#     current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     record['created_at'] = current_time
#     record['created_by'] = user_id if 'user_id' in globals() else None
#     record['updated_at'] = current_time
#     record['updated_by'] = user_id if 'user_id' in globals() else None
#     return record
#
#
# def validate_employer_id_check(employer_id, user_id, client_id, line):
#     status, value = validate_member_number(employer_id)
#
#     if not status:
#         return {"status": False, "errorMessage": value, "field": "Employer ID"}
#     print(line.values())
#     pdb.set_trace()
#     employer_name_value = line[1] if len(line) > 1 else line.get('employer_name', '')
#
#     if len(employer_name_value) > 40:
#         return {"status": False, "errorMessage": "Employer Name should be maximum 40 chars length.", "field": "Employer Name"}
#
#     new_record = prepare_employer_information(employer_id, user_id, client_id, employer_name_value)
#     return {"status": True, "duplicateStatus": False, "value": employer_id, "newRecord": new_record}
#
#
# def prepare_employer_information(employer_id, user_id, client_id, employer_name):
#     record = {
#         "id": '',
#         "client_id": client_id,
#         "employer_id": employer_id,
#         "employer_name": employer_name
#     }
#     record = set_default_attributes(user_id, record)
#     return record
#
#
# def employer_id_check(user_id, client_id):
#     def _employer_id_check(employer_id):
#         errors = []
#         if employer_id is not None:
#             employer_id = pd.DataFrame(employer_id)
#             for idx, emp_id in enumerate(employer_id):
#                 line = employer_id.iloc[idx].to_dict()
#                 check_status = validate_employer_id_check(emp_id, user_id, client_id, line)
#                 if not check_status['status']:
#                     errors.append(check_status['errorMessage'])
#         else:
#             errors.append("Chunk not provided.")
#
#         if errors:
#             return False
#
#         return True
#
#     return _employer_id_check



