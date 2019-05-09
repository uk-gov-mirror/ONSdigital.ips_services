from falcon import Request, Response

from ips.api.api import Api
import ips.services.dataimport.import_shift as imp


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class ImportShift(Api):

    # noinspection PyUnresolvedReferences
    def on_post(self, req: Request, resp: Response, run_id) -> None:
        v = req.get_param('ips-file')
        data = v.file.read()

        imp.import_shift(run_id, data)