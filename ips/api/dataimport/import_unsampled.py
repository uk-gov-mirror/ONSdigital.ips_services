from falcon import Request, Response

from ips.api.api import Api
import ips.services.dataimport.import_unsampled as imp


class ImportUnsampled(Api):

    def on_post(self, req: Request, resp: Response, run_id) -> None:
        v = req.get_param('ips-file')
        data = v.file.read()

        imp.import_unsampled_stream(data, run_id)