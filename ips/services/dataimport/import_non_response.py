import ips.persistence.import_non_response as nr

from ips.services import service


@service
def import_nonresponse_stream(run_id, data):
    return nr.import_nonresponse_from_stream(run_id, data)


@service
def import_nonresponse_file(run_id, nr_data_path):
    return nr.import_nonresponse_from_file(run_id, nr_data_path)
