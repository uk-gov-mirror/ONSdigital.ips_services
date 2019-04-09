import ips.persistence.import_shift as db
from ips.services import service


@service
def import_shift_stream(run_id, data):
    return db.import_shift_from_stream(run_id, data)


@service
def import_shift_file(run_id, nr_data_path):
    return db.import_shift_from_file(run_id, nr_data_path)
