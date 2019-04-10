import json

import falcon
from falcon import Request, Response
from ips_common.ips_logging import log

from ips.api.api import Api
from ips.api.validation.validate_run_id import validate_run_id
from ips.api.validation.validate import validate

from ips.persistence import data_management as db

from ips.services.apply_pv_service import apply_pvs


class ApplyPVsApi(Api):
    @validate(run_id=validate_run_id) # add validation for the pv_set
    def on_get(self, req: Request, resp: Response, run_id: str, pv_set: str) -> None:
        # Apply the PVs for a particular run

        log.debug("In Apply PVs")

        if not db.is_valid_run_id(run_id):
            result = {'status': "invalid job id: " + run_id}
            log.warning(f" Apply PVs: {run_id} does not exist")
            resp.status = falcon.HTTP_401
            resp.body = json.dumps(result)
            return

        # Call the apply function here
        # TODO: EEEEEELLLLLL!
        in_table_name = "dummy"
        out_table_name = "dummy"
        in_id = "dummy"
        dataset = "dummy"

        apply_pvs(in_table_name, out_table_name, in_id, dataset, run_id)
        resp.status = falcon.HTTP_201
