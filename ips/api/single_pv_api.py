from falcon import Request, Response, falcon
from ips.util.services_logging import log

from ips.api.api import Api
from ips.api.validation.validate import validate
from ips.api.validation.validate_run_id import validate_run_id
from ips.services.pv_service import edit_process_variable


class SinglePvApi(Api):

    @validate(run_id=validate_run_id)
    def on_post(self, req: Request, resp: Response, run_id: str) -> None:
        data = self.load_json_from_request(req)
        if 'RUN_ID' not in data:
            error = f"No JSON payload or run_id not stipulated."
            log.error(error)
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid request',
                                   'Could not decode the request body. The JSON was invalid.')

        response = edit_process_variable(run_id, data)

        if response:
            resp.status = falcon.HTTP_201
        else:
            resp.status = falcon.HTTP_500

