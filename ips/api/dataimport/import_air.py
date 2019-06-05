from falcon import Request, Response

from ips.api.api import Api
import ips.services.dataimport.import_traffic as imp
from ips.util.services_logging import log

# noinspection PyUnusedLocal,PyMethodMayBeStatic
class ImportAir(Api):

    # noinspection PyUnresolvedReferences
    def on_post(self, req: Request, resp: Response, run_id) -> None:
        log.debug(f"ImportAir: run_id: {run_id}")
        v = req.get_param('ips-file')
        data = v.file.read()

        month = req.get_param('month')
        year = req.get_param('year')

        imp.import_air(run_id, data, month, year)
