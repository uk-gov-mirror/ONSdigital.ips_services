import falcon
import io
from functools import partial

import pandas as pd
from ips_common.ips_logging import log

from ips.services.dataimport import validate_reference_data
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

    errors = Errors()
    validate_df = df.copy()

    validation = _validate_data(validate_df, month, year, errors, import_type)
    if not validation:
        log.error(f"Validation failed: {errors.get_messages()}")
        raise falcon.HTTPError(falcon.HTTP_400, 'data error', errors.get_messages())

    log.info(f"{import_type.name} validation completed successfully.")
    db.import_traffic_data(import_type, df, run_id)
    return df


# noinspection PyUnusedLocal
def _validate_data(data: pd.DataFrame, month, year, errors, import_type=None) -> bool:
    log.info(f"Validating {import_type.name} data...")
    return validate_reference_data.validate_data(import_type.name, data, month, year, errors)


class Errors:
    error_messages = []
    status = 0

    def add(self, message):
        self.error_messages.append(message)

    def get_messages(self):
        return self.error_messages


import_air = partial(_import_traffic, CSVType.Air)
import_sea = partial(_import_traffic, CSVType.Sea)
import_tunnel = partial(_import_traffic, CSVType.Tunnel)
