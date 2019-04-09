from functools import partial

import ips.persistence.import_traffic as db
from ips.services import service
from ips.services.dataimport import CSVType


@service
def _import_traffic_from_stream(import_type, run_id, data):
    return db.import_traffic_from_stream(import_type, run_id, data)


@service
def _import_traffic_from_file(import_type, run_id, survey_data_path):
    return db.import_traffic_from_file(import_type, run_id, survey_data_path)


import_air_file = partial(_import_traffic_from_file, CSVType.Air)
import_air_stream = partial(_import_traffic_from_stream, CSVType.Air)

import_sea_file = partial(_import_traffic_from_file, CSVType.Sea)
import_sea_stream = partial(_import_traffic_from_stream, CSVType.Sea)

import_tunnel_file = partial(_import_traffic_from_file, CSVType.Tunnel)
import_tunnel_stream = partial(_import_traffic_from_stream, CSVType.Tunnel)
