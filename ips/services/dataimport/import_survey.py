import ips.persistence.import_survey as db
from ips.services import service
from ips.persistence import apply_pvs_persistence as apply_pvs


@service
def import_survey_stream(run_id, data):
    db.import_survey_from_stream(run_id, data)
    return apply_survey_pvs(run_id)


@service
def import_survey_file(run_id, survey_data_path):
    db.import_survey_from_file(run_id, survey_data_path)
    return apply_survey_pvs(run_id)


@service
def apply_survey_pvs(run_id):
    return apply_pvs.apply_pvs_to_survey_data(run_id)
