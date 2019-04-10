import functools

import falcon
from ips_common.ips_logging import log


def service(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            log.debug(f"Calling service: {func.__name__}")
            return func(*args, **kwargs)

        except Exception as err:
            error = f'Error calling service {func.__name__}. Error: ' + str(err)
            log.error(error)
            raise falcon.HTTPError(falcon.HTTP_400, 'service error', error)

    return wrapper

