from falcon import Request, Response, falcon
from ips_common.ips_logging import log

from ips.api.api import Api
from ips.api.validation.validate import validate
from ips.api.validation.validate_run_id import validate_run_id
from ips.services.pv_service import get_process_variables, create_process_variables, delete_process_variables


class PvApi(Api):

    @validate(run_id=validate_run_id)
    def on_get(self, req: Request, resp: Response, run_id: str) -> None:
        resp.status = falcon.HTTP_200
        resp.body = get_process_variables(run_id)

    def on_post(self, req: Request, resp: Response, run_id: str) -> None:
        data = self.load_json_from_request(req)

        if 'RUN_ID' not in data:
            error = f"No JSON payload or run_id not stipulated."
            log.error(error)
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid request',
                                   'Could not decode the request body. The JSON was invalid.')

        create_process_variables(data, run_id)
        resp.status = falcon.HTTP_201

    @validate(run_id=validate_run_id)
    def on_delete(self, req: Request, resp: Response, run_id: str) -> None:
        resp.status = falcon.HTTP_200
        delete_process_variables(run_id)
