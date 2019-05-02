import io
import falcon
import pandas as pd
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
def import_survey_stream(run_id, data, month, year):
    log.info("Importing survey data from stream")
    return _import_survey(run_id, io.BytesIO(data), month, year)


@service
def import_survey_file(run_id, survey_data_path):
    log.info(f"Importing survey data from file: {survey_data_path}")
    return _import_survey(run_id, survey_data_path)


def _import_survey(run_id, source, month=None, year=None):
    df: pd.DataFrame = pd.read_csv(
        source,
        encoding="ISO-8859-1",
        engine="python",
        usecols=lambda x: x.upper() in columns,
        dtype=survey_data_schema.get_schema()
    )

    df.columns = df.columns.str.upper()
    if month is not None and year is not None:
        if not _validate_data(df, month, year):
            log.info("Validation failed. We need to implement an awesome exit strategy!")
    df = df.sort_values(by='SERIAL')
    db.import_survey_data(run_id, df)
    return df


def _validate_data(data: pd.DataFrame, user_month, user_year) -> bool:
    log.info("Validating survey data...")

    data_months = []
    data_years = []
    for index, row in data.iterrows():
        data_months.append(row['INTDATE'][-6:][:2])
        data_years.append(row['INTDATE'][-4:])

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

    if not all(elem in month for elem in data_months):
        error = f"Incorrect month select/uploaded."
        log.error(error)
        # return falcon.HTTPError(falcon.HTTP_401, 'data error', error)
        return False
    elif not all(elem in user_year for elem in data_years):
        error = f"Incorrect year select/uploaded."
        log.error(error)
        # return falcon.HTTPError(falcon.HTTP_401, 'data error', error)
        return False

    if 'SERIAL' not in data.columns:
        error = f"'SERIAL' column does not exist in data."
        log.error(error)
        # return falcon.HTTPError(falcon.HTTP_401, 'data error', error)
        return False

    # return falcon.HTTPError(falcon.HTTP_401, 'success')
    return True
