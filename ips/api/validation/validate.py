import functools
import inspect

import falcon
from ips_common.ips_logging import log


class ValidationProgrammingError(ValueError):
    def __init__(self, msg):
        super(ValidationProgrammingError, self).__init__(msg)


class ValidationFailed(ValueError):
    def __init__(self, msg):
        super(ValidationFailed, self).__init__(msg)


def _apply_rule(func, rule, param, getval):
    log.debug(f"Rule: function: {func.__name__}, rule: {rule.__name__}, param: {param} value: {getval(param)}")
    return rule(getval(param))


def throw_404(error: str):
    raise falcon.HTTPError(falcon.HTTP_400, 'Invalid request', error)


def validate(*name, **paramrules):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for rule in name:
                # Free rules *must* have a getter function to return
                # a value that can be passed into the validation
                # function
                if rule.getter is None:
                    msg = "Free rules must specify a getter. Rule={0}"
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

            missing_rules = list(param_names - rule_names)

            # TODO: for optimization, move this out to a
            # variable since it's immutable for our purposes
            # if missing_rules not in [[], ['self']]:
            #     msg = "Parameter(s) not validated {0}"
            #     msg = msg.format(missing_rules)
            #     raise ValidationProgrammingError(msg)

            unassigned_rules = list(rule_names - param_names)

            if unassigned_rules:
                msg = "No such parameter for rule(s) {0}"
                msg = msg.format(unassigned_rules)
                raise ValidationProgrammingError(msg)

            for param, rule in paramrules.items():
                getval = param_values.get

                if _apply_rule(func, rule, param, getval) is not None:
                    # Validation was not successful
                    return

            # Validation was successful, call the wrapped function

            return func(*args, **kwargs)

        return wrapper

    return decorator
