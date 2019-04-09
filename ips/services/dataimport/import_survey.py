import ips.persistence.import_survey as db
from ips.services import service


@service
def import_survey_stream(run_id, data):
    return db.import_survey_from_stream(run_id, data)


@service
def import_survey_file(run_id, survey_data_path):
    return db.import_survey_from_file(run_id, survey_data_path)
