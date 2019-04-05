from falcon import Request, Response

from ips.api.api import Api
from ips.api.validation.validate import validate
from ips.api.validation.validate_password import validate_password
from ips.api.validation.validate_user_id import validate_user_id
from ips.services.login_service import login


class LoginApi(Api):

    @validate(user_name=validate_user_id, password=validate_password)
    def on_get(self, req: Request, resp: Response, user_name: str, password: str) -> None:
        login(user_name, password)
