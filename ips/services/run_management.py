from ips_common.ips_logging import log

import ips.persistence.run_management as db

NOT_STARTED: int = 1
IN_PROGRESS: int = 2
DONE: int = 3
CANCELLED: int = 4
INVALID_RUN: int = 5
FAILED: int = 6


# set all in_progress runs to failed.
# called from the IPSWorkflow __init__ method at start up

def clear_existing_status():
    log.debug("Clearing IN_PROGRESS runs to FAILED")
    db.clear_existing_status()


def process_variables_exist(run_id: str) -> bool:
    return db.process_variables_exist(run_id)


def create_run(run_id: str):
    db.create_run(run_id=run_id, run_status=NOT_STARTED)


def is_in_progress(run_id: str) -> bool:
    return get_status(run_id) == IN_PROGRESS


def is_cancelled(run_id: str) -> bool:
    return get_status(run_id) == CANCELLED


def is_complete(run_id: str) -> bool:
    return ~ get_status(run_id) == IN_PROGRESS


def cancel_run(run_id: str) -> None:
    set_status(run_id, CANCELLED)


def set_status(run_id: str, status: int, step: str = None) -> None:
    db.set_status(run_id, status, step)


def set_step_status(run_id: str, status: int, step: str) -> None:
    db.set_step_status(run_id, status, step)


def reset_all_step_status(run_id):
    db.reset_steps(run_id)


def get_status(run_id: str) -> int:
    return db.get_run_status(run_id)


def get_step_status(run_id: str, step: str):
    return db.get_step_status(run_id, step)


def get_step(run_id) -> str:
    return db.get_step(run_id)


def set_percent_done(run_id: str, percent) -> None:
    db.set_percentage_done(run_id, percent)


def get_percent_done(run_id: str) -> int:
    return db.get_percentage_done(run_id)
