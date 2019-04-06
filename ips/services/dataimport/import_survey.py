import io

import pandas
from ips_common.logging import log

from ips.persistence.import_survey import insert_ss
from ips.services import service


@service
def import_survey_stream(data, run_id):
    df: pandas.DataFrame = pandas.read_csv(io.BytesIO(data), encoding="ISO-8859-1", engine="python")
    log.debug("Importing survey data")
    return _import_survey_data(df, run_id)


@service
def import_survey_file(survey_data_path: str, run_id: str) -> pandas.DataFrame:
    df: pandas.DataFrame = pandas.read_csv(survey_data_path, encoding="ISO-8859-1", engine="python")
    return _import_survey_data(df, run_id)


def _import_survey_data(df, run_id: str) -> pandas.DataFrame:
    """
    Author       : Thomas Mahoney
    Date         : 26 / 04 / 2018
    Purpose      : Loads the dataimport data into a dataframe then appends the data to the 'SURVEY_SUBSAMPLE'
                   table on the connected database.
    Parameters   : survey_data_path - the dataframe containing all of the dataimport data.
                   run_id - the generated run_id for the current run.
                   version_id - ID indicating the current version
    Returns      : NA
    Requirements : Datafile is of type '.csv', '.pkl' or '.sas7bdat'
    Dependencies : NA
    """

    # Fill left side of INTDATE column with an additional 0 if length less than 8 characters
    df.columns = df.columns.str.upper()
    if 'INTDATE' in df.columns:
        df['INTDATE'] = df['INTDATE'].astype(str).str.rjust(8, '0')

    # df.Day = df.Day.astype(str)

    # Call the extract data function to select only the needed columns.
    df_survey_data = _extract_data(df)

    # Add the generated run id to the dataset.
    df_survey_data['RUN_ID'] = pandas.Series(run_id, index=df_survey_data.index)

    # Insert the imported data into the survey_subsample table on the database.
    insert_ss(df_survey_data)
    # common_functions.insert_df_into_survey(df_survey_data)
    return df_survey_data


def _extract_data(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Author       : Thomas Mahoney
    Date         : 26 / 04 / 2018
    Purpose      : Extracts the required columns from the dataimport data.
    Parameters   : df - the dataframe containing all of the dataimport data.
    Returns      : The extracted dataframe.
    Requirements : NA
    Dependencies : NA
    """

    # Generate a list of the required columns
    columns = ['SERIAL', 'AM_PM_NIGHT', 'AGE', 'ANYUNDER16', 'APORTLATDEG',
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
               'TYPEINTERVIEW', 'SHIFT_WT']  # TODO: remove shift_wt here this is required for non-response step

    # Set the imported columns to be uppercase
    df.columns = df.columns.str.upper()

    # Sort the data by serial number
    df_new = df.sort_values(by='SERIAL')

    # Recreate the dataframe using only the specified columns
    df_new = df_new.filter(columns, axis=1)

    return df_new
