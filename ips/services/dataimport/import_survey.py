import io
import falcon
import pandas as pd
import numpy as np
from pandas.api.types import is_string_dtype
import ips.persistence.import_survey as db
from ips_common.ips_logging import log
from ips.services import service
from ips.services.dataimport.schemas import survey_data_schema

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
        validation = _validate_data(df, month, year)
        if not validation[0]:
            msg = validation[1]
            log.error(f"Validation failed: {msg}")
            raise falcon.HTTPError(falcon.HTTP_401, 'data error', msg)
        else:
            log.info("Validation completed successfully.")

    df = df.sort_values(by='SERIAL')
    db.import_survey_data(run_id, df)
    return df


def _validate_data(data: pd.DataFrame, user_month, user_year):

    def error_message():
        final_msg = ', '.join(map(str, msg))
        return resp, final_msg

    log.info("Validating Survey data...")
    msg = []
    resp = True

    if 'SERIAL' not in data.columns:
        msg.append(f"'SERIAL' column does not exist in Survey data.")
        resp = False
        return error_message()

    # Validate intdate
    if 'INTDATE' not in data.columns:
        msg.append(f"'INTDATE' column does not exist in Survey data.")
        resp = False
        return error_message()
    if not is_string_dtype(data['INTDATE']):
        data['INTDATE'] = data['INTDATE'].astype(str).str.rjust(8, '0')

    # Get the dates from the dataframe
    dates = _get_dates(data, user_month)
    months = list(map(int, dates[0]))
    data_months = list(map(int, dates[1]))
    data_years = dates[2]

    if not all(m in range(1, 13) for m in months):
        msg.append(f"Congratulations, you broke the internet! Invalid month selected.")
        resp = False
    if not all(y[:2] == '19' or y[:2] == '20' for y in data_years):
        msg.append(f"Unexpected year in INTDATE column of Survey data.")
        resp = False
    if not all(len(y) == 4 for y in data_years):
        msg.append(f"Invalid year in INTDATE column of Survey data. Invalid length.")
        resp = False

    # Do they match?
    if not all(m in range(1, 13) for m in data_months):
        msg.append(f"Invalid month in INTDATE column of Survey data.")
        resp = False
    elif not all(elem in months for elem in data_months):
        msg.append(f"Incorrect month selected or uploaded for Survey data.")
        resp = False

    if not all(elem in user_year for elem in data_years):
        msg.append(f"Incorrect year selected or uploaded for Survey data.")
        resp = False

    return error_message()


def _get_dates(data, user_month):
    data_months = []
    data_years = []
    for index, row in data.iterrows():
        data_months.append(row['INTDATE'][-6:][:2])
        data_years.append(row['INTDATE'][-4:])

    month = []
    if user_month[0] == 'Q':
        quarter = user_month[1]
        if quarter == '1':
            month = ['1', '2', '3']
        elif quarter == '2':
            month = ['4', '5', '6']
        elif quarter == '3':
            month = ['7', '8', '9']
        elif quarter == '4':
            month = ['10', '11', '12']
    else:
        month = [user_month]

    return month, data_months, data_years
