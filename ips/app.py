import logging

import falcon
from falcon_multipart.middleware import MultipartMiddleware
from ips_common.ips_logging import log

from ips.api.apply_pvs import ApplyPVsApi
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
from ips.api.pv_builder_api import PvBuilderApi, PvBuilderVariablesApi
from ips.api.pv_sets import PvSetsApi
from ips.api.run_api import RunApi
from ips.api.run_steps_api import RunStepsApi, RunStepsValueApi, RunStepsValueStepApi
from ips.api.start_service import StartApi
from ips.api.status_api import StatusApi
from ips.services import ips_workflow

workflow = ips_workflow.IPSWorkflow()

app = falcon.API(middleware=MultipartMiddleware())

status_api = StatusApi(workflow)
start_api = StartApi(workflow)
cancel_api = CancelApi(workflow)
apply_pvs = ApplyPVsApi(workflow)
run_api = RunApi(workflow)
run_steps_api = RunStepsApi(workflow)
run_steps_value_api = RunStepsValueApi(workflow)
run_steps_value_steps_api = RunStepsValueStepApi(workflow)

pv_sets_api = PvSetsApi(workflow)

login_api = LoginApi(workflow)

log.setLevel(logging.DEBUG)

pv_api = PvApi(workflow)
pv_builder_api = PvBuilderApi(workflow)
pv_builder_variables_api = PvBuilderVariablesApi(workflow)

import_survey = ImportSurvey(workflow)
import_air = ImportAir(workflow)
import_non_response = ImportNonResponse(workflow)
import_sea = ImportSea(workflow)
import_shift = ImportShift(workflow)
import_tunnel = ImportTunnel(workflow)
import_unsampled = ImportUnsampled(workflow)

export_api = ExportApi(workflow)

app.add_route('/ips-service/start/{run_id}', start_api)
app.add_route('/ips-service/status/{run_id}', status_api)
app.add_route('/ips-service/cancel/{run_id}', cancel_api)

app.add_route("/runs", run_api)

app.add_route("/run_steps/{run_id}", run_steps_api)
app.add_route("/run_steps/{run_id}/{value}", run_steps_value_api)
app.add_route("/run_steps/{run_id}/{value}/{step_number}", run_steps_value_steps_api)

app.add_route("/pv_sets", pv_sets_api)

app.add_route("/login/{user_name}/{password}", login_api)

app.add_route("/process_variables/{run_id}", pv_api)

app.add_route("/builder/{run_id}", pv_builder_api)
app.add_route("/builder/{run_id}/{pv_id}", pv_builder_api)
app.add_route("/builder/variables", pv_builder_variables_api)

app.add_route("/import/survey/{run_id}", import_survey)
app.add_route("/import/air/{run_id}", import_air)
app.add_route("/import/nonresponse/{run_id}", import_non_response)
app.add_route("/import/sea/{run_id}", import_sea)
app.add_route("/import/shift/{run_id}", import_shift)
app.add_route("/import/tunnel/{run_id}", import_tunnel)
app.add_route("/import/unsampled/{run_id}", import_unsampled)

app.add_route("/applypvs/{run_id}/{pv_set}", apply_pvs)

app.add_route("/export/{run_id}/{table_name}", export_api)

from waitress import serve

serve(app, host='0.0.0.0', port=5000, threads=4)
