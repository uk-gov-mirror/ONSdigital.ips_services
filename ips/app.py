import falcon
from falcon_multipart.middleware import MultipartMiddleware

from ips.api.health_check import HealthCheck
from ips.util.services_logging import log

from ips.api.cancel_api import CancelApi
from ips.api.dataimport.import_air import ImportAir
from ips.api.dataimport.import_non_response import ImportNonResponse
from ips.api.dataimport.import_sea import ImportSea
from ips.api.dataimport.import_shift import ImportShift
from ips.api.dataimport.import_survey import ImportSurvey
from ips.api.dataimport.import_tunnel import ImportTunnel
from ips.api.dataimport.import_unsampled import ImportUnsampled
from ips.api.export import ExportApi
from ips.api.login_api import LoginApi
from ips.api.pv_api import PvApi
from ips.api.pv_builder_api import PvBuilderApi
from ips.api.pv_sets import PvSetsApi
from ips.api.run_api import RunApi
from ips.api.run_steps_api import RunStepsApi, RunStepsValueApi, RunStepsValueStepApi
from ips.api.start_service import StartApi
from ips.api.status_api import StatusApi
from ips.api.test_pvs_api import PvTestApi

from ips.services import ips_workflow

workflow = ips_workflow.IPSWorkflow()

app = falcon.API(middleware=MultipartMiddleware())
app.req_options.auto_parse_form_urlencoded = True


app.add_route('/ips-service/start/{run_id}', StartApi(workflow))
app.add_route('/ips-service/status/{run_id}', StatusApi(workflow))
app.add_route('/ips-service/cancel/{run_id}', CancelApi(workflow))

app.add_route("/runs", RunApi(workflow))
app.add_route("/runs/{run_id}", RunApi(workflow))

app.add_route("/run_steps/{run_id}", RunStepsApi(workflow))
app.add_route("/run_steps/{run_id}/{value}", RunStepsValueApi(workflow))
app.add_route("/run_steps/{run_id}/{value}/{step_number}", RunStepsValueStepApi(workflow))

app.add_route("/pv_sets", PvSetsApi(workflow))

app.add_route("/login/{user_name}/{password}", LoginApi(workflow))
app.add_route("/health_check", HealthCheck(workflow))

app.add_route("/process_variables/{run_id}", PvApi(workflow))
app.add_route("/process_variables/test/{template}", PvTestApi(workflow))

app.add_route("/builder/{run_id}", PvBuilderApi(workflow))

app.add_route("/import/survey/{run_id}", ImportSurvey(workflow))
app.add_route("/import/air/{run_id}", ImportAir(workflow))
app.add_route("/import/nonresponse/{run_id}", ImportNonResponse(workflow))
app.add_route("/import/sea/{run_id}", ImportSea(workflow))
app.add_route("/import/shift/{run_id}", ImportShift(workflow))
app.add_route("/import/tunnel/{run_id}", ImportTunnel(workflow))
app.add_route("/import/unsampled/{run_id}", ImportUnsampled(workflow))

app.add_route("/export/{run_id}/{table_name}", ExportApi(workflow))

log.debug("IPS Services started")

# from waitress import serve
#
# serve(app, host='0.0.0.0', port=5000, threads=4)
