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
        # print("Employer Name should be maximum 40 chars length.")
        # pdb.set_trace()
        return {"status": False, "field": 'Employer Name', "errorMessage": "Employer Name should be maximum 40 chars length."}
    if not re.match(r"^[a-zA-Z0-9&._#$-/ ']*$", employer_name_value):
        # print('Employer Name contains invalid characters.')
        # pdb.set_trace()
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
        return True
    return _check





