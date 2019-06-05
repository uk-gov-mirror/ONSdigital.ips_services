from falcon import Request, Response, falcon
from ips.util.services_logging import log

from ips.api.api import Api
from ips.services.pv_sets import create_new_pv_set, get_pv_sets


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class PvSetsApi(Api):

    def on_post(self, req: Request, resp: Response, ) -> None:
        data = self.load_json_from_request(req)

        if 'RUN_ID' not in data:
            error = f"No JSON payload or run_id not stipulated in payload."
            log.error(error)
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid request',
                                   'Could not decode the request body. The JSON was invalid.')

        create_new_pv_set(data)
        resp.status = falcon.HTTP_201

    def on_get(self, req: Request, resp: Response) -> None:
        resp.status = falcon.HTTP_200
        resp.body = get_pv_sets()
