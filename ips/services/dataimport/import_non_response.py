import io
import falcon
import pandas as pd

from ips_common.ips_logging import log
import ips.persistence.import_non_response as nr
from ips.services import service
from ips.services.dataimport.schemas import non_response_schema
from ips.services.dataimport import validate_reference_data


@service
def import_nonresponse(run_id, data, month, year):
    log.info(f"Importing non_response data")
    df = pd.read_csv(
        io.BytesIO(data),
        encoding="ISO-8859-1",
        engine="python",
        dtype=non_response_schema.get_schema()
    )

    errors = Errors()
    validate_df = df.copy()

    validation = _validate_data(validate_df, month, year, errors)
    if not validation:
        log.error(f"Validation failed: {errors.get_messages()}")
        raise falcon.HTTPError(falcon.HTTP_400, 'data error', errors.get_messages())

    log.info("Validation completed successfully.")
    nr.import_non_response(run_id, df)
    return df


# noinspection PyUnusedLocal
def _validate_data(data: pd.DataFrame, month, year, errors) -> bool:
    log.info("Validating non response data...")

    # TODO: Get DATA_SOURCE_NAME from DATA_SOURCE
    reference_type = 'NON RESPONSE'
    return validate_reference_data.validate_data(reference_type, data, month, year, errors)


class Errors:
    error_messages = []
    status = 0

    def add(self, message):
        self.error_messages.append(message)

    def get_messages(self):
        return self.error_messages
