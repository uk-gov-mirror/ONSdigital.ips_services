from falcon import Request, Response

from ips.api.api import Api
import ips.services.dataimport.import_traffic as imp
from ips.util.services_logging import log


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class ImportTunnel(Api):

    def on_post(self, req: Request, resp: Response, run_id) -> None:
        log.debug(f"ImportTunnel: run_id: {run_id}")
        v = req.get_param('ips-file')
        data = v.file.read()

        month = req.get_param('month')
        year = req.get_param('year')

        imp.import_tunnel(run_id, data, month, year)
