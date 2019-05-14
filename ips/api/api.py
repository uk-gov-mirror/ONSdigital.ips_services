import json

import falcon
from falcon import Request, falcon
from ips_common.ips_logging import log

from ips.services.ips_workflow import IPSWorkflow


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class Api:
    def __init__(self, wf: IPSWorkflow):
        self.workflow = wf

    @staticmethod
    def load_json_from_request(req: Request) -> str:

        try:
            data = json.load(req.bounded_stream)
            if data is None:
                error = "No data. The request was empty"
                log.error(error)
                raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', error)
            return data

        except ValueError:
            error = "Could not decode the request body. The JSON was invalid."
            log.error(error)
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', error)
