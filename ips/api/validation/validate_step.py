from ips.api.validation.validate import ValidationFailed


def validate_step(value):
    if value is None:
        raise ValidationFailed("step cannot be empty")
