import ips.persistence.persistence as db
from ips.services.run_management import NOT_STARTED

RUN_MANAGEMENT = 'RUN_MANAGEMENT'
create_new_run = db.insert_into_table(RUN_MANAGEMENT)


def create_run(run_id: str, status: int) -> None:
    create_new_run(run_id=run_id, status=status, step=None, percent=0)


def set_status(run_id: str, status: int, step: str = None) -> None:
    if step is None:
        db.execute_sql()(
            f"UPDATE {RUN_MANAGEMENT} SET status = '{status}' WHERE RUN_ID='{run_id}' "
        )
    else:
        db.execute_sql()(
            f"UPDATE {RUN_MANAGEMENT} SET status = '{status}', STEP='{step}' WHERE RUN_ID='{run_id}' "
        )


def get_run_status(run_id: str) -> int:
    row = db.execute_sql()(
        f"SELECT STATUS FROM {RUN_MANAGEMENT} WHERE RUN_ID='{run_id}' "
    ).fetchone()

    if row is None:
        return NOT_STARTED

    return row['STATUS']


def get_step(run_id: str) -> str:
    row = db.execute_sql()(
        f"SELECT STEP FROM {RUN_MANAGEMENT} WHERE RUN_ID='{run_id}' "
    ).fetchone()

    return row['STEP']


def get_percentage_done(run_id: str) -> int:
    row = db.execute_sql()(
        f"SELECT PERCENT FROM {RUN_MANAGEMENT} WHERE RUN_ID='{run_id}' "
    ).fetchone()

    return row['PERCENT']


def set_percentage_done(run_id: str, percent: int) -> None:
    db.execute_sql()(
        f"UPDATE {RUN_MANAGEMENT} SET PERCENT = {percent} WHERE RUN_ID='{run_id}' "
    )
