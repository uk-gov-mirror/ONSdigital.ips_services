from ips.api.validation.validate import ValidationFailed
from ips.util.services_logging import log


def validate_step_value(value):
    if value is None:
        raise ValidationFailed("step value cannot be empty")
