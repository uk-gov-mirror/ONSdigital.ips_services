import time
import uuid

import ips_common_db.sql as db
from ips_common.ips_logging import log

from ips.persistence.import_shift import SHIFT_TABLE
from ips.services.dataimport.import_shift import  import_shift

nr_data = "data/import_data/dec/Poss shifts Dec 2017.csv"

run_id = str(uuid.uuid4())
start_time = time.time()


# noinspection PyUnusedLocal
def setup_module(module):
    log.info("Module level start time: {}".format(start_time))


def test_survey_import():
    log.info(f"-> Start shift data load for run_id: {run_id}")
    with open(nr_data, 'rb') as file:
        df = import_shift(run_id, file.read())
    log.info(f"-> End shift data load. {len(df)} rows.")


# noinspection PyUnusedLocal
def teardown_module(module):
    log.info("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))
    db.delete_from_table(SHIFT_TABLE, 'RUN_ID', '=', run_id)
