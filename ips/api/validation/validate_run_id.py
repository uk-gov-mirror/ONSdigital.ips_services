from ips.api.validation.validate import ValidationFailed


def validate_run_id(run_id):
    if run_id is None:
        raise ValidationFailed("run_id cannot be empty")
