
from ips.api.validation.validate import ValidationFailed


def validate_table(table):
    if table is None:
        raise ValidationFailed("table name cannot be empty")
