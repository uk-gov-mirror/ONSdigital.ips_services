import falcon
from ips_common.logging import log
from base64 import b64decode
from werkzeug.security import check_password_hash

from ips.persistence.persistence import read_table_values

get_users = read_table_values('user')


def login(user_name: str, password: str) -> None:

    data = get_users()

    user_credentials = data.loc[data['username'] == user_name]

    if user_credentials.empty:
        error = f"User, {user_name}, not found."
        log.error(error)
        raise falcon.HTTPError(falcon.HTTP_404, 'login error', error)

    password = b64decode(password.encode('ascii')).decode('ascii')

    if not check_password_hash(user_credentials['password'].values[0], password):
        error = f"Invalid password."
        log.error(error)
        raise falcon.HTTPError(falcon.HTTP_401, 'login error', error)

