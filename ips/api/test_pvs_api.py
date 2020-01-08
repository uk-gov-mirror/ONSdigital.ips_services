import json

import falcon
import typing
from falcon import Request, Response

from ips.api.api import Api
from ips.services.restricted_python import test_pvs, SuccessfulStatus, ErrorStatus


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class PvTestApi(Api):

    def on_get(self, req: Request, resp: Response, template: str) -> None:

        result = None
        r = test_pvs(template)
        if r is not None:
            if isinstance(r, SuccessfulStatus):
                result = {
                    'status': 'successful',
                    'template': template,
                }
            else:
                a = typing.Type[ErrorStatus]
                result = {
                    'status': a.status,
                    'template': a.template,
                    'errorMessage': a.errorMessage,
                    'PV': a.PV
                }
        resp.status = falcon.HTTP_200
        resp.body = json.dumps(result)
