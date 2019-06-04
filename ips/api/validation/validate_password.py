from ips.api.validation.validate import ValidationFailed
from ips.util.services_logging import log


def validate_password(password):
    if password is None:
        raise ValidationFailed("No password defined")
