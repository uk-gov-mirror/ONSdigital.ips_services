from falcon import Request, Response

from ips.api.api import Api
import ips.services.dataimport.import_shift as imp
from ips.util.services_logging import log


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class ImportShift(Api):

    # noinspection PyUnresolvedReferences
    def on_post(self, req: Request, resp: Response, run_id) -> None:
        log.debug(f"ImportShift: run_id: {run_id}")
        v = req.get_param('ips-file')
        data = v.file.read()

        month = req.get_param('month')
        year = req.get_param('year')

        imp.import_shift(run_id, data, month, year)
