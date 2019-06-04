import ips.persistence.pv_sets as pv_sets
from ips.services import service
from ips.util.services_logging import log


@service
def create_new_pv_set(data):
    pv_sets.create_pv_set(data)


@service
def get_pv_sets():
    return pv_sets.get_pv_sets()


