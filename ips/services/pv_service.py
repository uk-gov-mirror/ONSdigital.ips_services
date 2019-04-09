import ips.persistence.pv_persistence as pv
from ips.persistence.pv_persistence import delete_pv
from ips.services import service


@service
def get_process_variables(run_id=None):
    return pv.get_process_variables(run_id)


@service
def create_process_variables(data, run_id):
    pv.create_process_variables(data, run_id)


@service
def delete_process_variables(run_id):
    delete_pv(run_id=run_id)
