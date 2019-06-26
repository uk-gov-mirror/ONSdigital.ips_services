from falcon import Request, Response, falcon

from ips.api.api import Api
from ips.services.pv_builder_service import create_pv_build
from ips.util.services_logging import log


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class PvBuilderApi(Api):

    def on_post(self, req: Request, resp: Response, run_id) -> None:
        create_pv_build(req, run_id)
        resp.status = falcon.HTTP_201


