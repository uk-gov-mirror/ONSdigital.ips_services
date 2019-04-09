from base64 import b64decode

import falcon
from ips_common.logging import log
from werkzeug.security import check_password_hash

import ips.persistence.users as users


def login(user_name: str, password: str) -> None:
    user_credentials = users.get_user_details(user_name)

    if user_credentials.empty:
        error = f"User, {user_name}, not found."
        log.error(error)
        raise falcon.HTTPError(falcon.HTTP_404, 'login error', error)

    password = b64decode(password.encode('ascii')).decode('ascii')

    if not check_password_hash(user_credentials['password'].values[0], password):
        error = f"Invalid password."
        log.error(error)
        raise falcon.HTTPError(falcon.HTTP_401, 'login error', error)
