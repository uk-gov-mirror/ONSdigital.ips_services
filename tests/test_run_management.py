import time
import uuid

from ips_common.ips_logging import log
import ips.services.run_management as runs
import ips.persistence.run_management as runs_db
import ips.persistence.persistence as db

run_id = str(uuid.uuid4())
start_time = time.time()


def setup_module(module):
    log.info("Module level start time: {}".format(start_time))


def test_run_management():
    log.info(f"-> Start test run management for run_id: {run_id}")
    runs.create_run(run_id)
    status = runs.get_status(run_id)
    assert status == runs.NOT_STARTED

    runs.set_status(run_id, runs.IN_PROGRESS)
    status = runs.get_status(run_id)
    assert status == runs.IN_PROGRESS
    status = runs.is_complete(run_id)
    assert status is False

    message = 'THIS IS A TEST'
    runs.set_status(run_id, runs.IN_PROGRESS, step=message)
    status = runs.get_status(run_id)
    assert status == runs.IN_PROGRESS
    step = runs.get_step(run_id)
    assert step == message

    status = runs.is_in_progress(run_id)
    assert status is True

    runs.cancel_run(run_id)
    assert runs.is_cancelled(run_id) is True

    log.info(f"-> End test run management .")


def teardown_module(module):
    log.info("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))
    db.delete_from_table(runs_db.RUN_MANAGEMENT)(run_id=run_id)
