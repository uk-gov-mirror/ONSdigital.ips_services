import falcon
import io
from functools import partial

import pandas as pd
from ips.util.services_logging import log

from ips.services.dataimport import validate
import ips.persistence.import_traffic as db
from ips.services import service
from ips.services.dataimport import CSVType
from ips.services.dataimport.schemas import traffic_schema


@service
def _import_traffic(import_type, run_id, data, month, year):
    df: pd.DataFrame = pd.read_csv(
        io.BytesIO(data),
        encoding="ISO-8859-1",
        engine="python",
        dtype=traffic_schema.get_schema()
    )

    errors = Errors()
    validate_df = df.copy()

    try:
        validation = validate.validate_reference_data(import_type.name, validate_df, month, year, errors)
    except TypeError:
        errors.add(f"{import_type.name} file invalid or corrupt")
        log.error(f"Validation failed: {errors.get_messages()}")
        raise falcon.HTTPError(falcon.HTTP_400, 'file error', errors.get_messages())
    except Exception as err:
        errors.add(f"{import_type.name} file error: {err}")
        log.error(f"Validation failed: {errors.get_messages()}")
        raise falcon.HTTPError(falcon.HTTP_400, 'file error', errors.get_messages())

    if not validation:
        log.error(f"{import_type.name} data validation failed: {errors.get_messages()}")
        raise falcon.HTTPError(falcon.HTTP_400, 'data error', errors.get_messages())

    log.info(f"{import_type.name} validation completed successfully")
    db.import_traffic_data(import_type, df, run_id)
    return df


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
