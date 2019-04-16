import ips.persistence.apply_pvs_persistence as builder
from ips.services import service


@service
def apply_pvs(run_id):
    return builder.apply_pvs_to_survey_data(run_id)
