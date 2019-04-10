from ips.api.validation.validate import ValidationFailed

valid_tables = [
    "SURVEY_SUBSAMPLE",
    "PS_FINAL",
    "SHIFT_DATA",
    "NON_RESPONSE_DATA",
    "PS_SHIFT_DATA",
    "PS_NON_RESPONSE",
    "PS_MINIMUMS",
    "PS_TRAFFIC",
    "PS_UNSAMPLED_OOH",
    "PS_IMBALANCE"
]


def validate_table(table_name):
    if table_name not in valid_tables:
        raise ValidationFailed(f"table [{table_name}] is invalid")
