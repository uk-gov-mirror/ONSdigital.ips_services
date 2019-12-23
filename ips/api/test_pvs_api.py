import json

import falcon
from falcon import Request, Response

from ips.api.api import Api
from ips.services.restricted_python import test_pvs


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class PvTestApi(Api):

    def on_get(self, req: Request, resp: Response, template: str) -> None:
        try:
            test_pvs(template)
            result = {
                'status': 'successful',
            }
            resp.status = falcon.HTTP_200
            resp.body = json.dumps(result)

        except Exception as err:
            result = {
                'status': f"compile/exec of template {template} failed",
                'template': template,
                'error': str(err)
            }
            resp.status = falcon.HTTP_401
            resp.body = json.dumps(result)
