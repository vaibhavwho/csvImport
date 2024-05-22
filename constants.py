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
CLAIM_TYPE = {
    'M': 'Medical',
    'D': 'Dental',
    'V': 'Vision',
    'H': 'HRA',
    'O': 'Other'
}
INPATIENT_OR_OUTPATIENT = {
    'I': 'Inpatient',
    'O': 'Outpatient',
    'E': 'Emergency'
}