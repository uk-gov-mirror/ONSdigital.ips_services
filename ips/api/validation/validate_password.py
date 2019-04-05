from ips.api.validation.validate import ValidationFailed


def validate_password(password):
    if password is None:
        raise ValidationFailed("No password defined")
