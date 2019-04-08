import ips.persistence.import_unsampled as db

from ips.services import service


@service
def import_unsampled_stream(import_type, run_id, data):
    return db.import_unsampled_from_stream(import_type, run_id, data)


@service
def import_unsampled_file(run_id, survey_data_path):
    return db.import_unsampled_from_file(run_id, survey_data_path)
