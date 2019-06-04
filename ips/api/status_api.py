import json

import falcon
from falcon import Request, Response

from ips.api.api import Api
from ips.api.validation.validate import validate
from ips.api.validation.validate_run_id import validate_run_id
from ips.persistence import data_management as db
from ips.persistence.persistence import get_responses
from ips.util.services_logging import log


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class StatusApi(Api):

    @validate(run_id=validate_run_id)
    def on_get(self, req: Request, resp: Response, run_id: str) -> None:
        # Return status for a specified  run ID

        if not db.is_valid_run_id(run_id):
            result = {'status': "invalid job id: " + run_id}
            resp.status = falcon.HTTP_401
            resp.body = json.dumps(result)
            return

        v = str(self.workflow.get_status(run_id))

        result = {
            'status': v,
            'percentage_done': self.workflow.get_percentage_done(run_id),
            'steps': {}
        }
        for x in range(1, 15):
            result['steps'][str(x)] = {}
            result['steps'][str(x)]['Status'] = str(self.workflow.get_step_status(run_id, str(x)))
            responses = json.loads(get_responses(str(x), run_id))
            if len(responses) > 0:
                result['steps'][str(x)]['Responses'] = responses

        resp.body = json.dumps(result)
