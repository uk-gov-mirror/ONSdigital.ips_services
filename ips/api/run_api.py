import falcon
from falcon import Request, Response
from ips_common.ips_logging import log

from ips.api.api import Api
from ips.api.validation.validate import validate
from ips.api.validation.validate_run_id import validate_run_id
from ips.services.runs_service import create_run, edit_run, get_run


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class RunApi(Api):

    def on_get(self, req: Request, resp: Response) -> str:
        res = get_run()
        resp.body = res
        return get_run()

    @validate(run_id=validate_run_id)
    def on_put(self, req: Request, resp: Response, run_id: str) -> None:
        data = self.load_json_from_request(req)
        edit_run(run_id, data)

    def on_post(self, req: Request, resp: Response) -> None:
        data = self.load_json_from_request(req)

        if 'RUN_ID' not in data:
            error = f"No JSON payload or run_id not stipulated in payload."
            log.error(error)
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid request',
                                   'Could not decode the request body. The JSON was invalid.')
        create_run(data)
        resp.status = falcon.HTTP_201
