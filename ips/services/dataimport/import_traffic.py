import io
from functools import partial

import pandas as pd
from ips_common.ips_logging import log

import ips.persistence.import_traffic as db
from ips.services import service
from ips.services.dataimport import CSVType
from ips.services.dataimport.schemas import traffic_schema


@service
def _import_traffic(import_type, run_id, data, month, year):
    log.info("Importing traffic data")
    df: pd.DataFrame = pd.read_csv(
        io.BytesIO(data),
        encoding="ISO-8859-1",
        engine="python",
        dtype=traffic_schema.get_schema()
    )

    _validate_data(df)
    db.import_traffic_data(import_type, df, run_id)
    return df


# noinspection PyUnusedLocal
def _validate_data(data: pd.DataFrame) -> bool:
    pass


import_air = partial(_import_traffic, CSVType.Air)
import_sea = partial(_import_traffic, CSVType.Sea)
import_tunnel = partial(_import_traffic, CSVType.Tunnel)
