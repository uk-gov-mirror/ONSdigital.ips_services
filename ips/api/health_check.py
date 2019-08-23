from falcon import Request, Response, falcon

from ips.api.api import Api


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class HealthCheck(Api):

    def on_get(self, req: Request, resp: Response) -> None:
        resp.status = falcon.HTTP_200
