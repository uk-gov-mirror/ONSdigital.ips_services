import json

import falcon
from falcon import Request, Response

from ips.api.api import Api
from ips.api.validation.validate_run_id import validate_run_id
from ips.api.validation.validate_table import validate_table
from ips.api.validation.validate import validate
from ips.services.export_service import get_export_data


class ExportApi(Api):

    @validate(run_id=validate_run_id, table_name=validate_table)
    def on_get(self, req: Request, resp: Response, run_id: str, table_name: str) -> None:

        res = get_export_data(run_id, table_name)

        if res is None:
            s = f"invalid run id: {run_id} or table: {table_name} is empty"
            result = {'status': s}
            resp.body = json.dumps(result)
            resp.status = falcon.HTTP_404
        else:
            resp.status = falcon.HTTP_200
            resp.body = json.dumps(res)
