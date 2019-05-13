import falcon
import io
import pandas as pd
import ips.persistence.import_shift as db

from ips.services.dataimport import validate_reference_data
from ips_common.ips_logging import log
from ips.services import service
from ips.services.dataimport.schemas import shift_schema


@service
def import_shift(run_id, data, month, year):
    log.info("Importing shift data")
    df = pd.read_csv(
        io.BytesIO(data),
        encoding="ISO-8859-1",
        engine="python",
        dtype=shift_schema.get_schema()
    )

    errors = Errors()
    validate_df = df.copy()

    validation = _validate_data(validate_df, month, year, errors)
    if not validation:
        log.error(f"Validation failed: {errors.get_messages()}")
        raise falcon.HTTPError(falcon.HTTP_400, 'data error', errors.get_messages())

        log.info("Validation completed successfully.")

    db.import_shift_data(run_id, df)
    return df


# noinspection PyUnusedLocal
def _validate_data(data: pd.DataFrame, month, year, errors) -> bool:
    log.info("Validating shift data...")

    # TODO: Get DATA_SOURCE_NAME from DATA_SOURCE
    reference_type = 'SHIFT'
    return validate_reference_data.validate_data(reference_type, data, month, year, errors)


class Errors:
    error_messages = []
    status = 0

    def add(self, message):
        self.error_messages.append(message)

    def get_messages(self):
        return self.error_messages

