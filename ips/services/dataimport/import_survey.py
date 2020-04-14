import io

import falcon
import numpy as np
import pandas as pd
from ips.util.services_logging import log

import ips.persistence.import_survey as db
from ips.services import service
from ips.services.dataimport import validate

columns = [
    'SERIAL', 'AM_PM_NIGHT', 'AGE', 'ANYUNDER16', 'APORTLATDEG',
    'APORTLATMIN', 'APORTLATSEC', 'APORTLATNS', 'APORTLONDEG',
    'APORTLONMIN', 'APORTLONDSEC', 'APORTLONEW', 'ARRIVEDEPART',
    'BABYFARE', 'BEFAF', 'CHILDFARE', 'CHANGECODE', 'COUNTRYVISIT',
    'CPORTLATDEG', 'CPORTLATMIN', 'CPORTLATSEC', 'CPORTLATNS',
    'CPORTLONDEG', 'CPORTLONMIN', 'CPORTLONDSEC', 'CPORTLONEW',
    'INTDATE', 'DAYTYPE', 'DIRECTLEG', 'DVEXPEND', 'DVFARE',
    'DVLINECODE', 'DVPACKAGE', 'PACKAGECOST', 'DVPACKCOST', 'DVPERSONS', 'DVPORTCODE',
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
    try:
        df: pd.DataFrame = pd.read_csv(
            io.BytesIO(data),
            encoding="ISO-8859-1",
            engine="python",
            skipinitialspace=True,
            usecols=lambda x: x.upper() in columns
        )

        def convert_col_to_int(data_frame, col):
            data_frame[col] = data_frame[col].fillna(-1).astype(int).replace('-1', np.nan)

        df.columns = df.columns.str.upper()
        df['TANDTSI'] = df['TANDTSI'].round(0)
        [convert_col_to_int(df, x) for x in ['EXPENDITURE', 'TANDTSI']]
        errors = Errors()
        validation = validate.validate_survey_data(df, month, year, errors)
        if not validation:
            log.error(f"Validation failed: {errors.get_messages()}")
            raise falcon.HTTPError(falcon.HTTP_400, 'data error', errors.get_messages())
        log.info("Survey validation completed successfully.")

        df = df.sort_values(by='SERIAL')
        db.import_survey_data(run_id, df)
        return df
    except Exception as e:
        log.error(f"Validation failed: {e}")
        raise falcon.HTTPError(falcon.HTTP_400, 'data error', e)


def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    df = df.applymap(trim_strings)
    replace_with_null = lambda x: None if x == '' else x
    return df.applymap(replace_with_null)


class Errors:

    def __init__(self):
        self.error_messages = []

    def add(self, message):
        self.error_messages.append(message)

    def get_messages(self):
        return self.error_messages
