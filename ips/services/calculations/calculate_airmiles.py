import math

import numpy as np
import pandas as pd
from pandas import DataFrame
from ips.util.services_logging import log
from ips.services.calculations import parallelise_dataframe

FLOW_VARIABLE = 'FLOW'
DIST1 = 'UKLEG'
DIST2 = 'OVLEG'
DIST3 = 'DIRECTLEG'

AIR1_COLUMNS = [
    'PROUTELATDEG', 'PROUTELATMIN', 'PROUTELATSEC',
    'PROUTELONDEG', 'PROUTELONMIN', 'PROUTELONSEC',
    'APORTLATDEG', 'APORTLATMIN', 'APORTLATSEC',
    'APORTLONDEG', 'APORTLONMIN', 'APORTLONSEC',
    'PROUTELATNS', 'APORTLATNS',
    'PROUTELONEW', 'APORTLONEW'
]

AIR2_COLUMNS = [
    'CPORTLATDEG', 'CPORTLATMIN', 'CPORTLATSEC',
    'CPORTLONDEG', 'CPORTLONMIN', 'CPORTLONSEC',
    'APORTLATDEG', 'APORTLATMIN', 'APORTLATSEC',
    'APORTLONDEG', 'APORTLONMIN', 'APORTLONSEC',
    'CPORTLATNS', 'APORTLATNS', 'CPORTLONEW',
    'APORTLONEW'
]

AIR3_COLUMNS = [
    'PROUTELATDEG', 'PROUTELATMIN', 'PROUTELATSEC',
    'PROUTELONDEG', 'PROUTELONMIN', 'PROUTELONSEC',
    'CPORTLATDEG', 'CPORTLATMIN', 'CPORTLATSEC',
    'CPORTLONDEG', 'CPORTLONMIN', 'CPORTLONSEC',
    'PROUTELATNS', 'CPORTLATNS', 'PROUTELONEW',
    'CPORTLONEW'
]

AIRMILES_COLUMNS = [
    'START_LAT_DEGREE', 'START_LAT_MIN', 'START_LAT_SEC',
    'START_LON_DEGREE', 'START_LON_MIN', 'START_LON_SEC',
    'END_LAT_DEGREE', 'END_LAT_MIN', 'END_LAT_SEC',
    'END_LON_DEGREE', 'END_LON_MIN', 'END_LON_SEC',
    'START_LAT_DIR', 'END_LAT_DIR', 'START_LON_DIR',
    'END_LON_DIR'
]


def calculate_airmiles(df_air_ext: DataFrame) -> DataFrame:
    """
    Author       : Thomas Mahoney / Nassir Mohammad
    Date         : 19 / 09 / 2018
    Purpose      : Calculates the air miles values for the given data set.
    Parameters   : df_air_ext - A data frame containing the information needed to
                                produce an air miles output.
    Returns      : A data frame containing the air miles calculation produced and
                   the associated record's serial number.
    Requirements : NA
    Dependencies : NA
    """

    # convert all None Type to NaN
    df_air_ext.fillna(value=np.nan, inplace=True)

    # run this in parallel
    df_air_ext = parallelise_dataframe(df_air_ext, extract_rows)
    # df_air_ext = extract_rows(df_air_ext)

    # Selects and returns the calculated air miles and serials
    df_air_ext = df_air_ext[['SERIAL', 'AIRMILES']]

    return df_air_ext


def extract_rows(df):
    return df.apply(get_airmiles, axis=1)


def get_airmiles(row):
    sec_60: int = 60
    sec_3600: int = 3600
    earth_diameter: int = 7918
    pi: float = 3.141592654
    pi2: float = 6.283185307
    deg_rad_factor: float = 0.017453292

    # Set seconds to zero if they are missing to allow calculation to proceed
    if math.isnan(row['START_LAT_SEC']):
        row['START_LAT_SEC'] = 0
    if math.isnan(row['END_LAT_SEC']):
        row['END_LAT_SEC'] = 0
    if math.isnan(row['START_LON_SEC']):
        row['START_LON_SEC'] = 0
    if math.isnan(row['END_LON_SEC']):
        row['END_LON_SEC'] = 0

    lat1 = row['START_LAT_DEGREE'] + (((row['START_LAT_MIN'] * sec_60) + row['START_LAT_SEC']) / sec_3600)
    lat2 = row['END_LAT_DEGREE'] + (((row['END_LAT_MIN'] * sec_60) + row['END_LAT_SEC']) / sec_3600)
    lon1 = row['START_LON_DEGREE'] + (((row['START_LON_MIN'] * sec_60) + row['START_LON_SEC']) / sec_3600)
    lon2 = row['END_LON_DEGREE'] + (((row['END_LON_MIN'] * sec_60) + row['END_LON_SEC']) / sec_3600)

    lat1_rad = lat1 * deg_rad_factor
    lat2_rad = lat2 * deg_rad_factor
    lon1_rad = lon1 * deg_rad_factor
    lon2_rad = lon2 * deg_rad_factor

    if row['START_LON_DIR'] == row['END_LON_DIR']:
        lon_diff_rad = abs(lon1_rad - lon2_rad)
    else:
        lon_diff_rad = (lon1_rad + lon2_rad)

        if lon_diff_rad > pi:
            lon_diff_rad = pi2 - lon_diff_rad

    cos_lon_diff = math.cos(lon_diff_rad)
    sin_lat1 = math.sin(lat1_rad)
    sin_lat2 = math.sin(lat2_rad)
    cos_lat1 = math.cos(lat1_rad)
    cos_lat2 = math.cos(lat2_rad)

    if row['START_LAT_DIR'] == 'S':
        sin_lat1 = sin_lat1 * (-1)

    if row['END_LAT_DIR'] == 'S':
        sin_lat2 = sin_lat2 * (-1)

    cosx = (sin_lat1 * sin_lat2) + (cos_lat1 * cos_lat2 * cos_lon_diff)

    if cosx > 1:
        cosx = 1

    tan_halfx = math.sqrt((1 - cosx) / (1 + cosx))
    atan_halfx = math.atan(tan_halfx)

    if not math.isnan(atan_halfx):
        row['AIRMILES'] = round((atan_halfx * earth_diameter))

    return row


def do_ips_airmiles_calculation(df_surveydata: DataFrame, var_serial: str) -> DataFrame:
    """
    Author       : Thomas Mahoney
    Date         : 01 / 03 / 2018
    Purpose      : Creates and prepares the air miles data set extracts before 
                   beginning the calculation function. 
    Parameters   : df_surveydata - the imported survey sub-sample.                           
                   var_serial - variable holding the serial number column reference
    Returns      : A data frame containing all three air miles calculations
    Requirements : NA
    Dependencies : NA
    """

    # Select rows from the imported data that have the correct FLOW_VARIABLE value
    df_airmiles = df_surveydata[df_surveydata[FLOW_VARIABLE].isin((1, 2, 3, 4))]

    # Create data frames from the specified column sets
    df_air_ext1 = df_airmiles[[var_serial] + AIR1_COLUMNS]
    df_air_ext2 = df_airmiles[[var_serial] + AIR2_COLUMNS]
    df_air_ext3 = df_airmiles[[var_serial] + AIR3_COLUMNS]

    # Rename the dataframe's columns in preparation for the air miles calculation.
    df_air_ext1 = df_air_ext1.rename(columns=dict(zip(AIR1_COLUMNS, AIRMILES_COLUMNS)))
    df_air_ext2 = df_air_ext2.rename(columns=dict(zip(AIR2_COLUMNS, AIRMILES_COLUMNS)))
    df_air_ext3 = df_air_ext3.rename(columns=dict(zip(AIR3_COLUMNS, AIRMILES_COLUMNS)))

    # Calculate the air miles values for all the data sets created    
    df_air1 = calculate_airmiles(df_air_ext1)
    df_air2 = calculate_airmiles(df_air_ext2)
    df_air3 = calculate_airmiles(df_air_ext3)

    # Rename the air miles results for each generated data set  
    df_airmiles1 = df_air1.rename(columns={'AIRMILES': DIST1})
    df_airmiles2 = df_air2.rename(columns={'AIRMILES': DIST2})
    df_airmiles3 = df_air3.rename(columns={'AIRMILES': DIST3})

    # Sort the air miles data frames before merging
    df_airmiles1 = df_airmiles1.sort_values(var_serial)
    df_airmiles2 = df_airmiles2.sort_values(var_serial)
    df_airmiles3 = df_airmiles3.sort_values(var_serial)

    # Merge the air miles data frames by their 'SERIAL' column
    df_airmiles_merged = pd.merge(df_airmiles1, df_airmiles2, on=var_serial, how='left')
    df_airmiles_merged = df_airmiles_merged.sort_values(var_serial)
    df_airmiles_merged = pd.merge(df_airmiles_merged, df_airmiles3, on=var_serial, how='left')

    # Sort and return the merged data
    df_airmiles_merged.sort_values(var_serial)

    return df_airmiles_merged
