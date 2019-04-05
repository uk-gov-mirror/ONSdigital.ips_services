
from ips.api.validation.validate import ValidationFailed


def validate_step_value(value):
    if value is None:
        raise ValidationFailed("step value cannot be empty")
