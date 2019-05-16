import json
import threading

import falcon
from falcon import Request, Response
from ips_common.ips_logging import log

from ips.api.api import Api
from ips.api.validation.validate import validate
from ips.api.validation.validate_run_id import validate_run_id
from ips.persistence import data_management as db
import ips.services.run_management as runs


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class StartApi(Api):

    @validate(run_id=validate_run_id)
    def on_put(self, req: Request, resp: Response, run_id: str) -> None:
        # Start a run

        if self.workflow.is_in_progress():
            error = f"Can only run one instance of a workflow at a time, {run_id} rejected."
            log.error(error)
            raise falcon.HTTPError(falcon.HTTP_403, 'Concurrency Error', error)

        log.info("Starting calculations for RUN_ID: " + run_id)

        try:
            if not db.is_valid_run_id(run_id):
                result = {'status': "invalid job id: " + run_id}
                resp.status = falcon.HTTP_401
                resp.body = json.dumps(result)
                return

            if not runs.process_variables_exist(run_id):
                error = f"cannot start run: {run_id} as process variables for the run don't exist"
                log.error(error)
                raise falcon.HTTPError(falcon.HTTP_400, 'Invalid request', error)

            thr = threading.Thread(target=self.workflow.run_calculations, args=(run_id,))

            thr.start()

            log.info(f"started job: {run_id}")

            result = {'status': "started job: " + run_id}
            resp.body = json.dumps(result)

        except ValueError:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON',
                                   'Could not decode the request body. The JSON was invalid.')
