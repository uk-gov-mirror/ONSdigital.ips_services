import time
import uuid

import ips_common_db.sql as db
from ips_common.ips_logging import log

from ips.persistence.import_non_response import NON_RESPONSE_TABLE
from ips.services.dataimport.import_non_response import import_nonresponse

nr_data = "data/import_data/dec/Dec17_NR.csv"

run_id = str(uuid.uuid4())
start_time = time.time()


# noinspection PyUnusedLocal
def setup_module(module):
    log.info("Module level start time: {}".format(start_time))


def test_survey_import():
    log.info(f"-> Start NR data load for run_id: {run_id}")
    with open(nr_data, 'rb') as file:
        df = import_nonresponse(run_id, file.read())
    log.info(f"-> End NR data load. {len(df)} rows.")


# noinspection PyUnusedLocal
def teardown_module(module):
    log.info("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))
    db.delete_from_table(NON_RESPONSE_TABLE, 'RUN_ID', '=', run_id)
