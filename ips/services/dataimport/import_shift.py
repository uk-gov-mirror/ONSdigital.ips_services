import io
import pandas as pd
import falcon
import ips.persistence.import_shift as db
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

    df.columns = df.columns.str.upper()

    errors = Errors()
    validation = _validate_data(df, month, year, errors)
    if not validation:
        log.error(f"Validation failed: {errors.get_messages()}")
        raise falcon.HTTPError(falcon.HTTP_400, 'data error', errors.get_messages())

        log.info("Validation completed successfully.")

    db.import_shift_data(run_id, df)
    return df


def _validate_data(data: pd.DataFrame, user_month, user_year, errors):
    log.info("Validating shift data...")

    data['DATASOURCE'] = data['DATASOURCE'].str.upper()
    datasource_col = data['DATASOURCE'].astype(str)

    for index, row in data.iterrows():
        datasource = row['DATASOURCE']

        if 'SHIFT' not in datasource:
            log.error("Data does not match shift. Exiting validation.")
            errors.add("Data does not match shift.")
            return False

    return _validate_date(data, user_month, user_year, errors)


def _validate_date(data, user_month, user_year, errors):
    valid_quarters = {
        "Q1": ['1', '2', '3'], "Q2": ['4', '5', '6'], "Q3": ['7', '8', '9'], "Q4": ['10', '11', '12']
    }

    year_col = data['YEAR'].astype(str)
    month_col = data['MONTH'].astype(str)

    for index, row in data.iterrows():
        year = str(row['YEAR'])

        if not str.isdigit(year) or not 2000 <= int(year) <= 2099:
            errors.add(f"year value [{year}] in survey data stream is invalid")
            return False

        if not str.isdigit(user_year) or int(user_year) != int(year):
            errors.add(f"user supplier year value [{user_year}] is invalid")
            return False

    for index, row in data.iterrows():
        month = str(row['MONTH'])

        if user_month in valid_quarters and month not in valid_quarters[user_month]:
            errors.add(f"user supplied quarter [{user_month}] does not correspond to valid month in data [{month}]")
            return False

        if not str.isdigit(month) or not 1 <= int(month) <= 12:
            errors.add(f"data month value [{month}] in data stream is invalid")
            return False

        if int(month) != int(user_month):
            errors.add(f"user supplied month [{user_month}] does not correspond to data month [{month}]")
            return False

    return True


class Errors:
    error_messages = []
    status = 0

    def add(self, message):
        self.error_messages.append(message)

    def get_messages(self):
        return self.error_messages

