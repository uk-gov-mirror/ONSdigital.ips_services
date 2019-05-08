import falcon
from falcon import Request, Response

from ips.api.api import Api
from ips.api.validation.validate import validate
from ips.api.validation.validate_run_id import validate_run_id
from ips.api.validation.validate_step import validate_step
from ips.api.validation.validate_step_value import validate_step_value
from ips.services.run_steps_service import create_run_steps, edit_run_steps, get_run_steps


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class RunStepsApi(Api):

    @validate(run_id=validate_run_id)
    def on_get(self, req: Request, resp: Response, run_id: str) -> None:
        resp.body = get_run_steps(run_id)

    @validate(run_id=validate_run_id)
    def on_post(self, req: Request, resp: Response, run_id: str) -> None:
        resp.status = falcon.HTTP_201
        create_run_steps(run_id)


# noinspection PyUnusedLocal
class RunStepsValueApi(Api):

    @validate(run_id=validate_run_id)
    def on_put(self, req: Request, resp: Response, run_id: str, value: str) -> None:
        edit_run_steps(run_id, value)
        resp.status = falcon.HTTP_200


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class RunStepsValueStepApi(Api):

    @validate(run_id=validate_run_id, step_value=validate_step_value, step=validate_step)
    def on_put(self, req: Request, resp: Response, run_id: str, step_value: str, step: str) -> None:
        edit_run_steps(run_id, step_value, step)
        resp.status = falcon.HTTP_200
