import io
import pandas as pd
import falcon
import ips.persistence.import_unsampled as db
from ips.util.services_logging import log
from ips.services import service
from ips.services.dataimport.schemas import unsampled_schema
from ips.services.dataimport import validate
from ips.services.dataimport import CSVType


@service
def import_unsampled(run_id, data, month, year):
    df = pd.read_csv(
        io.BytesIO(data),
        encoding="ISO-8859-1",
        engine="python",
        dtype=unsampled_schema.get_schema()
    )

    errors = Errors()
    validate_df = df.copy()

    validation = validate.validate_reference_data(CSVType.Unsampled.name, validate_df, month, year, errors)
    if not validation:
        log.error(f"Validation failed: {errors.get_messages()}")
        raise falcon.HTTPError(falcon.HTTP_400, 'data error', errors.get_messages())

    log.info(f"{CSVType.Unsampled.name} validation completed successfully")
    db.import_unsampled(run_id, df)
    return df


class Errors:
    error_messages = []
    status = 0

    def add(self, message):
        self.error_messages.append(message)

    def get_messages(self):
        return self.error_messages
