import io

import falcon
import numpy as np
import pandas as pd
from ips_common.ips_logging import log

import ips.persistence.import_survey as db
from ips.services import service

columns = [
    'SERIAL', 'AM_PM_NIGHT', 'AGE', 'ANYUNDER16', 'APORTLATDEG',
    'APORTLATMIN', 'APORTLATSEC', 'APORTLATNS', 'APORTLONDEG',
    'APORTLONMIN', 'APORTLONDSEC', 'APORTLONEW', 'ARRIVEDEPART',
    'BABYFARE', 'BEFAF', 'CHILDFARE', 'CHANGECODE', 'COUNTRYVISIT',
    'CPORTLATDEG', 'CPORTLATMIN', 'CPORTLATSEC', 'CPORTLATNS',
    'CPORTLONDEG', 'CPORTLONMIN', 'CPORTLONDSEC', 'CPORTLONEW',
    'INTDATE', 'DAYTYPE', 'DIRECTLEG', 'DVEXPEND', 'DVFARE',
    'DVLINECODE', 'DVPACKAGE', 'DVPACKCOST', 'DVPERSONS', 'DVPORTCODE',
    'EXPENDCODE', 'EXPENDITURE', 'FARE', 'FAREK', 'FLOW', 'HAULKEY',
    'INTENDLOS', 'INTMONTH', 'KIDAGE', 'LOSKEY', 'MAINCONTRA', 'MIGSI',
    'NATIONALITY', 'NATIONNAME', 'NIGHTS1', 'NIGHTS2', 'NIGHTS3', 'NIGHTS4',
    'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8', 'NUMADULTS', 'NUMDAYS',
    'NUMNIGHTS', 'NUMPEOPLE', 'PACKAGEHOL', 'PACKAGEHOLUK', 'PERSONS',
    'PORTROUTE', 'PACKAGE', 'PROUTELATDEG', 'PROUTELATMIN', 'PROUTELATSEC',
    'PROUTELATNS', 'PROUTELONDEG', 'PROUTELONMIN', 'PROUTELONSEC',
    'PROUTELONEW', 'PURPOSE', 'QUARTER', 'RESIDENCE', 'RESPNSE',
    'SEX', 'SHIFTNO', 'SHUTTLE', 'SINGLERETURN', 'TANDTSI', 'TICKETCOST',
    'TOWNCODE1', 'TOWNCODE2', 'TOWNCODE3', 'TOWNCODE4', 'TOWNCODE5',
    'TOWNCODE6', 'TOWNCODE7', 'TOWNCODE8', 'TRANSFER', 'UKFOREIGN',
    'VEHICLE', 'VISITBEGAN', 'WELSHNIGHTS', 'WELSHTOWN', 'FAREKEY',
    'TYPEINTERVIEW'
]


@service
def import_survey(run_id, data, month, year):
    log.info("Importing survey data")
    df: pd.DataFrame = pd.read_csv(
        io.BytesIO(data),
        encoding="ISO-8859-1",
        engine="python",
        usecols=lambda x: x.upper() in columns
    )

    def convert_col_to_int(data_frame, col):
        data_frame[col] = data_frame[col].fillna(-1).astype(int).replace('-1', np.nan)

    df.columns = df.columns.str.upper()
    [convert_col_to_int(df, x) for x in ['EXPENDITURE', 'DVEXPEND', 'TANDTSI']]

    if month is not None and year is not None:
        errors = Errors()
        validation = _validate_data(df, month, year, errors)
        if not validation:
            log.error(f"Validation failed: {errors.get_messages()}")
            raise falcon.HTTPError(falcon.HTTP_400, 'data error', errors.get_messages())

        log.info("Validation completed successfully.")

    df = df.sort_values(by='SERIAL')
    db.import_survey_data(run_id, df)
    return df


def _validate_data(data: pd.DataFrame, user_month, user_year, errors):
    log.info("Validating Survey data...")

    # Validate SERIAL column exists
    if 'SERIAL' not in data.columns:
        log.error(f"'SERIAL' column does not exist. Exiting validation.")
        errors.add("'SERIAL' column does not exist in Survey data.")
        return False

    # Validate INTDATE column exists
    if 'INTDATE' not in data.columns:
        log.error("'INTDATE' column does not exist. Exiting validation.")
        errors.add("'INTDATE' column does not exist in Survey data.")
        return False

    date_column = data['INTDATE'].astype(str).str.rjust(8, '0')

    return _validate_date(date_column, user_month, user_year, errors)


def _validate_date(data, user_month, user_year, errors):
    valid_quarters = {
        "Q1": ['1', '2', '3'], "Q2": ['4', '5', '6'], "Q3": ['7', '8', '9'], "Q4": ['10', '11', '12']
    }

    for index, row in data.iteritems():
        year = row[-4:]
        month = row[-6:][:2]

        if not str.isdigit(year) or not 2000 <= int(year) <= 2099:
            errors.add(f"year value [{year}] in data stream is invalid")
            return False

        if not str.isdigit(user_year) or int(user_year) != int(year):
            errors.add(f"user supplier year value [{user_year}] is invalid")
            return False

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
