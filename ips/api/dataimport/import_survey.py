from falcon import Request, Response

from ips.api.api import Api
import ips.services.dataimport.import_survey as imp


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class ImportSurvey(Api):

    def on_post(self, req: Request, resp: Response, run_id) -> None:
        v = req.get_param('ips-file')
        data = v.file.read()

        month = req.get_param('month')
        year = req.get_param('year')

        imp.import_survey_stream(run_id, data, month, year)

