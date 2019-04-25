from ips.api.validation.validate import ValidationFailed


def validate_user_id(user_id):
    if user_id is None:
        raise ValidationFailed("Username cannot be empty")
