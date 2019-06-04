import ips.persistence.runs_persistence as runs
from ips.services import service
from ips.util.services_logging import log


@service
def get_run():
    return runs.get_run()


@service
def create_run(data):
    runs.create_run(data)


@service
def edit_run(run_id, data):
    runs.edit_run(run_id, data)
