connection_string = 'mysql+pymysql://root:@localhost/sir'

CLAIM_STATUS = [
    ('A', 'Void of a coordination of benefits adjustment'),
    ('B', 'Coordination of Benefits'),
    ('C', 'Check Voided'),
    ('P', 'Paid service line'),
    ('R', 'Refund from provider'),
    ('V', 'Voided service line'),
    ('E', 'Paid service line item that is excluded from reinsurance contracts'),
    ('F', 'Correction of a prior claim item'),
    ('Z', 'Pended Claim'),
    ('P', 'Closed'),
]

CLAIM_CAUSE = {
    'A': 'Accident',
    'C': 'Chemical',
    'D': 'Dental',
    'I': 'Illness',
    'M': 'Maternity',
    'P': 'Psychiatric',
    'W': 'Wellness'
}

CLAIM_TYPE = {
    'M': 'Medical',
    'D': 'Dental',
    'V': 'Vision',
    'H': 'HRA',
    'O': 'Other'
}

BENEFIT_ASSIGNED = {'Y': 'Yes', 'N': 'No'}

INPATIENT_OR_OUTPATIENT = {
    'I': 'Inpatient',
    'O': 'Outpatient',
    'E': 'Emergency'
}

CLAIM_FORM_TYPE = {'P': 'Professional', 'F': 'Institutional Facility'}

SERVICE_TYPE = 15

SSNTRIGGER = 'ssn_trigger'
CONDITIONS = 'condition'
PROCEDURES = 'procedure'
PROVIDERS = 'provider'
MEMBERS = 'member'
LOADDATA = 'loadData'

isMajorCategory = 'MAJOR CATEGORY'
isSubDiagnosis = 'SUBDIAGNOSIS'
isChronic = 'CHRONIC CONDITIONS'
isTriggerDiagnosis = 'TRIGGER DIAGNOSIS'
IN_DB_CONDITION = True

CLAIM_MEMBER_COLUMNS = ['subscriber_type', 'employee_status', 'gender', 'dob', 'state', 'address', 'city', 'zip', 'latitude', 'longitude', 'member_name', 'member_id']