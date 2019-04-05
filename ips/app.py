import logging

import falcon
from ips_common.logging import log

from ips.api.cancel_api import CancelApi
from ips.api.login_api import LoginApi
from ips.api.pv_api import PvApi
from ips.api.pv_builder_api import PvBuilderApi, PvBuilderVariablesApi
from ips.api.run_api import RunApi
from ips.api.dataimport.import_survey import ImportSurvey
from ips.api.run_steps_api import RunStepsApi, RunStepsValueApi, RunStepsValueStepApi
from ips.api.start_service import StartApi
from ips.api.status_api import StatusApi
from ips.services import ips_workflow
from falcon_multipart.middleware import MultipartMiddleware

workflow = ips_workflow.IPSWorkflow()

app = falcon.API(middleware=MultipartMiddleware())

status_api = StatusApi(workflow)
start_api = StartApi(workflow)
cancel_api = CancelApi(workflow)

run_api = RunApi(workflow)
run_steps_api = RunStepsApi(workflow)
run_steps_value_api = RunStepsValueApi(workflow)
run_steps_value_steps_api = RunStepsValueStepApi(workflow)

login_api = LoginApi(workflow)

log.setLevel(logging.DEBUG)

pv_api = PvApi(workflow)
pv_builder_api = PvBuilderApi(workflow)
pv_builder_variables_api = PvBuilderVariablesApi(workflow)

import_survey = ImportSurvey(workflow)

app.add_route('/ips-service/start/{run_id}', start_api)
app.add_route('/ips-service/status/{run_id}', status_api)
app.add_route('/ips-service/cancel/{run_id}', cancel_api)

app.add_route("/runs", run_api)

app.add_route("/run_steps/{run_id}", run_steps_api)
app.add_route("/run_steps/{run_id}/{value}", run_steps_value_api)
app.add_route("/run_steps/{run_id}/{value}/{step_number}", run_steps_value_steps_api)

app.add_route("/login/{user_name}/{password}", login_api)

app.add_route("/process_variables/{run_id}", pv_api)

app.add_route("/builder/{run_id}/{pv_id}", pv_builder_api)
app.add_route("/builder/variables", pv_builder_variables_api)

app.add_route("/import/survey/{run_id}", import_survey)
