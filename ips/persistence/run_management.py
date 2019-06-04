import ips.persistence.persistence as db
import ips.services.run_management as status
from ips.util.services_logging import log

RUN_STEPS = 'RUN_STEPS'
RUN_MANAGEMENT = 'RUN'
PROCESS_VARIABLES = 'PROCESS_VARIABLE_PY'


def clear_existing_status():
    db.execute_sql()(
        f"UPDATE {RUN_MANAGEMENT} SET RUN_STATUS = {status.FAILED} WHERE RUN_STATUS={status.IN_PROGRESS} "
    )


def create_run(run_id: str, run_status: int) -> None:
    db.execute_sql()(
        f"UPDATE {RUN_MANAGEMENT} SET RUN_STATUS = {run_status} WHERE RUN_ID='{run_id}' "
    )


def process_variables_exist(run_id: str) -> bool:
    row = db.execute_sql()(
        f"SELECT RUN_ID FROM {PROCESS_VARIABLES} WHERE RUN_ID='{run_id}' "
    ).fetchone()

    if row is None:
        return False

    return True


def is_existing_run(run_id: str) -> bool:
    row = db.execute_sql()(
        f"SELECT RUN_ID FROM {RUN_MANAGEMENT} WHERE RUN_ID='{run_id}' "
    ).fetchone()

    if row is None:
        return False

    return True


def set_status(run_id: str, run_status: int, step: str = None) -> None:
    if step is None:
        db.execute_sql()(
            f"UPDATE {RUN_MANAGEMENT} SET RUN_STATUS = {run_status} WHERE RUN_ID='{run_id}' "
        )
    else:
        db.execute_sql()(
            f"UPDATE {RUN_MANAGEMENT} SET RUN_STATUS = {run_status}, STEP='{step}' WHERE RUN_ID='{run_id}' "
        )


def set_step_status(run_id: str, step_status: int, step: str = None) -> None:
    if step is not None:
        db.execute_sql()(
            f"UPDATE {RUN_STEPS} SET STEP_STATUS = {step_status} WHERE RUN_ID='{run_id}' AND STEP_NUMBER = '{step}'"
        )


def reset_steps(run_id: str) -> None:
    db.execute_sql()(
        f"UPDATE {RUN_STEPS} SET STEP_STATUS = 0 WHERE RUN_ID='{run_id}'"
    )


def cancel_steps(run_id: str) -> None:
    db.execute_sql()(
        f"UPDATE {RUN_STEPS} SET STEP_STATUS = 4 WHERE RUN_ID='{run_id}' AND STEP_STATUS = 0"
    )


def get_step_status(run_id: str, step: str) -> int:
    row = db.execute_sql()(
        f"SELECT STEP_STATUS FROM {RUN_STEPS} WHERE RUN_ID='{run_id}' AND STEP_NUMBER = '{step}'"
    ).fetchone()

    if row is None:
        return status.NOT_STARTED
    return row['STEP_STATUS']


def get_run_status(run_id: str) -> int:
    row = db.execute_sql()(
        f"SELECT RUN_STATUS FROM {RUN_MANAGEMENT} WHERE RUN_ID='{run_id}' "
    ).fetchone()

    if row is None:
        return status.NOT_STARTED
    return row['RUN_STATUS']


def get_step(run_id: str) -> str:
    row = db.execute_sql()(
        f"SELECT STEP FROM {RUN_MANAGEMENT} WHERE RUN_ID='{run_id}' "
    ).fetchone()

    if row is None:
        return "Step not Started"

    return row['STEP']


def get_percentage_done(run_id: str) -> int:
    row = db.execute_sql()(
        f"SELECT PERCENT FROM {RUN_MANAGEMENT} WHERE RUN_ID='{run_id}' "
    ).fetchone()

    if row is None:
        return status.NOT_STARTED

    return row['PERCENT']


def set_percentage_done(run_id: str, percent: int) -> None:
    db.execute_sql()(
        f"UPDATE {RUN_MANAGEMENT} SET PERCENT = {percent} WHERE RUN_ID='{run_id}' "
    )


def insert_issue(run_id: str, step_num: int, response_code: int, msg: str):
    db.execute_sql()(
        f"INSERT INTO RESPONSE (RUN_ID, STEP_NUMBER, RESPONSE_CODE, MESSAGE) "
        f"VALUES ('{run_id}', {step_num}, {response_code}, '{msg}')"
    )


def reset_issues(run_id: str):
    db.execute_sql()(
        f"DELETE FROM RESPONSE WHERE RUN_ID = '{run_id}'"
    )
