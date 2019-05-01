import io
from functools import partial

import pandas as pd
from ips_common.ips_logging import log

import ips.persistence.import_traffic as db
from ips.services import service
from ips.services.dataimport import CSVType
from ips.services.dataimport.schemas import traffic_schema


@service
def _import_traffic_from_stream(import_type, run_id, data):
    log.info("Importing traffic data from stream")
    return _import_traffic(import_type, run_id, io.BytesIO(data))


@service
def _import_traffic_from_file(import_type, run_id, data):
    log.info(f"Importing traffic data from file: {data}")
    return _import_traffic(import_type, run_id, data)


def _import_traffic(import_type, run_id, data):
    df: pd.DataFrame = pd.read_csv(
        data,
        encoding="ISO-8859-1",
        engine="python",
        dtype=traffic_schema.get_schema()
    )

    _validate_data(df)
    db.import_traffic_data(import_type, df, run_id)
    return df


def _validate_data(data: pd.DataFrame) -> bool:
    pass


import_air_file = partial(_import_traffic_from_file, CSVType.Air)
import_air_stream = partial(_import_traffic_from_stream, CSVType.Air)

import_sea_file = partial(_import_traffic_from_file, CSVType.Sea)
import_sea_stream = partial(_import_traffic_from_stream, CSVType.Sea)

import_tunnel_file = partial(_import_traffic_from_file, CSVType.Tunnel)
import_tunnel_stream = partial(_import_traffic_from_stream, CSVType.Tunnel)
