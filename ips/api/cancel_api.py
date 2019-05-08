import json

import falcon
from falcon import Request, Response

from ips.api.api import Api
from ips.api.validation.validate_run_id import validate_run_id
from ips.api.validation.validate import validate

from ips.persistence import data_management as db


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class CancelApi(Api):

    @validate(run_id=validate_run_id)
    def on_get(self, req: Request, resp: Response, run_id: str) -> None:
        # Cancel the specified run

        if not db.is_valid_run_id(run_id):
            result = {'status': "invalid job id: " + run_id}
            resp.status = falcon.HTTP_401
            resp.body = json.dumps(result)
            return

        self.workflow.cancel_run(run_id)

