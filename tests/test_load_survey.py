import time
import uuid

import ips_common_db.sql as db
from ips_common.ips_logging import log

from ips.persistence.import_survey import SURVEY_SUBSAMPLE
from ips.services.dataimport.import_survey import import_survey

survey_data = "data/import_data/dec/surveydata.csv"

run_id = str(uuid.uuid4())
start_time = time.time()


# noinspection PyUnusedLocal
def setup_module(module):
    log.info("Module level start time: {}".format(start_time))


def test_survey_import():
    log.info(f"-> Start survey data load for run_id: {run_id}")
    with open(survey_data, 'rb') as file:
        df = import_survey(run_id, file.read(), None, None)
    log.info(f"-> End survey data load. {len(df)} rows.")


# noinspection PyUnusedLocal
def teardown_module(module):
    log.info("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))
    db.delete_from_table(SURVEY_SUBSAMPLE, 'RUN_ID', '=', run_id)
