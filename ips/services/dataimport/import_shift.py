import falcon
import io
import pandas as pd
import ips.persistence.import_shift as db

from ips.services.dataimport import validate
from ips.util.services_logging import log
from ips.services import service
from ips.services.dataimport.schemas import shift_schema
from ips.services.dataimport import CSVType


@service
def import_shift(run_id, data, month, year):
    df = pd.read_csv(
        io.BytesIO(data),
        encoding="ISO-8859-1",
        engine="python",
        dtype=shift_schema.get_schema()
    )

    errors = Errors()
    validate_df = df.copy()

    try:
        validation = validate.validate_reference_data(CSVType.Shift.name, validate_df, month, year, errors)
    except TypeError:
        errors.add(f"{CSVType.Shift.name} file invalid or corrupt")
        log.error(f"Validation failed: {errors.get_messages()}")
        raise falcon.HTTPError(falcon.HTTP_400, 'file error', errors.get_messages())
    except Exception as err:
        errors.add(f"{CSVType.Shift.name} file error: {err}")
        log.error(f"Validation failed: {errors.get_messages()}")
        raise falcon.HTTPError(falcon.HTTP_400, 'file error', errors.get_messages())

    if not validation:
        log.error(f"Validation failed: {errors.get_messages()}")
        raise falcon.HTTPError(falcon.HTTP_400, 'data error', errors.get_messages())

    log.info(f"{CSVType.Shift.name} validation completed successfully")
    db.import_shift_data(run_id, df)
    return df


class Errors:
    error_messages = []
    status = 0

    def add(self, message):
        self.error_messages.append(message)

    def get_messages(self):
        return self.error_messages
