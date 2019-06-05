import functools
import inspect

import falcon
from ips.util.services_logging import log


class ValidationProgrammingError(ValueError):
    def __init__(self, msg):
        super(ValidationProgrammingError, self).__init__(msg)


class ValidationFailed(ValueError):
    def __init__(self, msg):
        super(ValidationFailed, self).__init__(msg)


def _apply_rule(func, rule, param, getval):
    log.debug(f"Rule: function: {func.__name__}, rule: {rule.__name__}, param: {param}, value: {getval(param)}")
    return rule(getval(param))


def invalid_parameter(error: str):
    raise falcon.HTTPError(status=falcon.HTTP_412, title='Invalid parameter', description=error)


def validate(*name, **paramrules):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for rule in name:
                # Free rules *must* have a getter function to return
                # a value that can be passed into the validation
                # function
                if rule.getter is None:
                    msg = "Rules must specify a getter. Rule={0}"
                    msg = msg.format(rule.__class__.__name__)
                    raise ValidationProgrammingError(msg)
            funcparams = inspect.getfullargspec(func)

            param_map = list(zip(funcparams.args, args))

            # Create dictionary that maps parameters passed
            # to their values passed
            param_values = dict(param_map)

            # Bring in kwargs so that we can validate those as well.
            param_values.update(kwargs)

            # Now check for rules and parameters. We should have one
            # rule for every parameter.
            param_names = set(param_values.keys())
            rule_names = set(paramrules.keys())

            unassigned_rules = list(rule_names - param_names)

            if unassigned_rules:
                msg = "No such parameter for rule(s) {0}"
                msg = msg.format(unassigned_rules)
                raise ValidationProgrammingError(msg)

            for param, rule in paramrules.items():
                getval = param_values.get

                try:
                    if _apply_rule(func, rule, param, getval) is not None:
                        raise ValidationFailed(f"Validation Failed")
                except ValidationFailed as ve:
                    raise invalid_parameter(f"Validation failed for parameter: [{param}], error: {str(ve)}")

            # Validation was successful, call the wrapped function

            return func(*args, **kwargs)

        return wrapper

    return decorator
