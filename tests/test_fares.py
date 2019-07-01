import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
import math

from ips.util.services_logging import log
from pandas import DataFrame, Series
from ips.services.calculations import ips_impute

from ips.persistence import data_management as idm
from ips.persistence.persistence import insert_from_dataframe, truncate_table
from ips.util.services_configuration import ServicesConfiguration
from ips.persistence.persistence import select_data
from ips.services.calculations.sas_random import SASRandom
from ips.services.calculations.sas_rounding import ips_rounding


def test_fares():
    survey_subsample_table = 'SURVEY_SUBSAMPLE'
    config = ServicesConfiguration().get_fares_imputation()
    run_id = 'h3re-1s-y0ur-run-1d'

    # Load survey data
    survey_data = pd.read_csv("data/calculations/december_2017/fares/fares_calculation_input.csv")
    survey_data.drop(survey_data.columns[0], inplace=True, axis=1)
    truncate_table("SAS_SURVEY_SUBSAMPLE")()
    insert_from_dataframe('SAS_SURVEY_SUBSAMPLE')(survey_data)

    # Run fares calculation and subsequent db steps
    survey_data_out = do_ips_fares_imputation(survey_data,
                                              var_serial='SERIAL',
                                              num_levels=9,
                                              measure='mean')

    insert_from_dataframe(config["temp_table"])(survey_data_out)
    idm.update_survey_data_with_step_results(config)
    idm.store_survey_data_with_step_results(run_id, config)

    # Start testing shizznizz
    # TODO --->
    output_columns = ['SERIAL', 'FARE', 'FAREK', 'SPEND', 'SPENDIMPREASON']

    # Create comparison survey dataframes
    survey_subsample = select_data("*", survey_subsample_table, "RUN_ID", run_id)

    survey_results = survey_subsample[output_columns].copy()
    survey_expected = pd.read_csv(
        "data/calculations/december_2017/fares/surveydata_fares.csv")
    survey_expected = survey_expected[output_columns].copy()

    # pandas.testing.faff
    survey_results.sort_values(by='SERIAL', axis=0, inplace=True)
    survey_results.index = range(0, len(survey_results))

    survey_expected.sort_values(by='SERIAL', axis=0, inplace=True)
    survey_expected.index = range(0, len(survey_expected))

    # TODO: run this and it should produce a result of...
    assert_frame_equal(survey_results, survey_expected, check_dtype=False, check_less_precise=True)


INTDATE = 'INTDATE'
DVFARE = 'DVFARE'
FARE = 'FARE'
FARES_IMP_ELIGIBLE_PV = 'FARES_IMP_ELIGIBLE_PV'
FARES_IMP_FLAG_PV = 'FARES_IMP_FLAG_PV'
FAREK = 'FAREK'
FAGE_PV = 'FAGE_PV'
BABYFARE = 'BABYFARE'
CHILDFARE = 'CHILDFARE'
APD_PV = 'APD_PV'
DVPACKAGE = 'DVPACKAGE'
DISCNT_F2_PV = 'DISCNT_F2_PV'
QMFARE_PV = 'QMFARE_PV'
PACKAGECOST = 'PACKAGECOST'
DVPACKCOST = 'DVPACKCOST'
DISCNT_PACKAGE_COST_PV = 'DISCNT_PACKAGE_COST_PV'
DVPERSONS = 'DVPERSONS'
PERSONS = 'PERSONS'
DVEXPEND = 'DVEXPEND'
EXPENDITURE = 'EXPENDITURE'
BEFAF = 'BEFAF'
SPEND = 'SPEND'
SPENDIMPREASON = 'SPENDIMPREASON'
DUTY_FREE_PV = 'DUTY_FREE_PV'
PACKAGE = 'PACKAGE'

# Setup thresh and strata base nested lists. These are used to group the data
# differently at each iteration.
THRESH_BASE_LIST = [3, 3, 3, 3, 3, 3, 3, 0, 0]

STRATA_BASE_LIST = [
    ['INTMONTH', 'TYPE_PV', 'UKPORT1_PV', 'OSPORT1_PV', 'OPERA_PV'],
    ['INTMONTH', 'TYPE_PV', 'UKPORT2_PV', 'OSPORT1_PV', 'OPERA_PV'],
    ['INTMONTH', 'TYPE_PV', 'UKPORT1_PV', 'OSPORT2_PV', 'OPERA_PV'],
    ['INTMONTH', 'TYPE_PV', 'UKPORT2_PV', 'OSPORT2_PV', 'OPERA_PV'],
    ['INTMONTH', 'TYPE_PV', 'UKPORT3_PV', 'OSPORT2_PV', 'OPERA_PV'],
    ['INTMONTH', 'TYPE_PV', 'UKPORT3_PV', 'OSPORT3_PV', 'OPERA_PV'],
    ['INTMONTH', 'TYPE_PV', 'UKPORT4_PV', 'OSPORT3_PV', 'OPERA_PV'],
    ['INTMONTH', 'TYPE_PV', 'UKPORT4_PV', 'OSPORT4_PV'],
    ['INTMONTH', 'TYPE_PV', 'OSPORT4_PV']
]


def do_ips_fares_imputation(df_input: DataFrame, var_serial: str, num_levels: int, measure: str) -> DataFrame:
    log.debug("Starting fares imputation")

    sas_random = SASRandom(123456)

    for index, row in df_input.iterrows():

        if row['FLOW'] > 4:
            df_input.at[index, 'OPERA_PV'] = 3
        else:
            df_input.at[index, 'OPERA_PV'] = round(sas_random.random(), 0) + 1

    df_eligible = df_input.loc[df_input['FARES_IMP_ELIGIBLE_PV'] == 1.0]

    # Replace blank values with 'NOTHING' as python drops blanks during the aggregation process.
    df_eligible['INTMONTH'].fillna(99, inplace=True)

    df_eligible['DVPACKCOST'].fillna(0, inplace=True)
    df_eligible['PERSONS'].fillna(0, inplace=True)
    df_eligible['DVEXPEND'].fillna(0, inplace=True)
    df_eligible['FARE'].fillna(0, inplace=True)
    df_eligible['BEFAF'].fillna(0, inplace=True)

    # Perform the imputation on eligible dataset
    df_output = ips_impute.ips_impute(df_eligible, var_serial,
                                      STRATA_BASE_LIST, THRESH_BASE_LIST,
                                      num_levels, DVFARE, FARE,
                                      measure, FARES_IMP_FLAG_PV,
                                      FAREK)

    # Merge df_output_final and df_input by var_serial_num

    df_output.sort_values(var_serial, inplace=True)

    df_input.sort_values(var_serial, inplace=True)

    # df_output = df_input.merge(df_output, on=var_serial, how='left')
    df_output = df_input.merge(df_output, how='left', left_on=var_serial, right_on=var_serial)

    # Above merge creates fares_x and fares_y column; this line removes the empty
    # fares_x column and keeps then renames the imputed fares_y column
    df_output = df_output.drop([FARE + '_x', FAREK + '_x'], axis=1)

    df_output.rename(index=str, columns={FARE + '_y': FARE, FAREK + '_y': FAREK}, inplace=True)

    df_output = df_output.apply(compute_additional_fares, axis=1)

    df_output = df_output.apply(compute_additional_spend, axis=1)

    return df_output[[var_serial, SPEND, SPENDIMPREASON, FARE, FAREK]]


def calculate_non_pack_fare(row: Series) -> float:
    intdate = row[INTDATE]
    a = str(intdate)
    intdate = a.zfill(8)

    day = int(intdate[:2])
    month = int(intdate[2:4])
    year = int(intdate[4:8])

    fage_pv = row[FAGE_PV]
    baby_fare = row[BABYFARE]
    child_fare = row[CHILDFARE]
    apd_pv = row[APD_PV]
    fare = row[FARE]

    if fage_pv == 1:
        return baby_fare * (fare - apd_pv)

    if fage_pv == 2:
        # Ensure date is on or later than the 1st of May 2016
        # This is because APD for under 16's was removed from this date.
        if year >= 2016 and month >= 5 and day >= 1:
            return child_fare * (fare - apd_pv)
        else:
            return (child_fare * (fare - apd_pv)) + apd_pv

    if fage_pv == 6:
        return fare

    return np.NaN


def compute_additional_fares(row: Series):
    fares_imp_flag_pv = row[FARES_IMP_FLAG_PV]
    fares_imp_eligible_pv = row[FARES_IMP_ELIGIBLE_PV]

    dvfare = row[DVFARE]

    dv_package = row[DVPACKAGE]
    discnt_f2 = row[DISCNT_F2_PV]
    qm_fare = row[QMFARE_PV]

    # Sort out child/baby fares
    def calculate_fare():
        non_pack_fare = calculate_non_pack_fare(row)

        # Compute package versions of fare
        if dv_package in (1, 2):
            if math.isnan(non_pack_fare) or math.isnan(discnt_f2):
                return np.NaN
            else:
                return ips_rounding(non_pack_fare * discnt_f2, 0)
        else:
            return ips_rounding(non_pack_fare, 0)

    if fares_imp_flag_pv == 0 or fares_imp_eligible_pv == 0:
        fare = dvfare
    else:
        fare = calculate_fare()

    if not isinstance(fare, (float, int)):
        fare = pd.to_numeric(fare, errors="coerce")
    if not isinstance(qm_fare, (float, int)):
        qm_fare = pd.to_numeric(qm_fare, errors="coerce")

    # Test for Queen Mary fare
    if np.isnan(fare) and not np.isnan(qm_fare):
        fare = qm_fare

    # Ensure the fare is rounded to nearest integer
    row[FARE] = ips_rounding(fare, 0)

    return row


def compute_additional_spend(row):
    dv_package = row[DVPACKAGE]
    package = row[PACKAGE]
    befaf = row[BEFAF]
    fare = row[FARE]
    duty_free = row[DUTY_FREE_PV]
    package_cost = row[DVPACKCOST]
    discounted_package_cost = row[DISCNT_PACKAGE_COST_PV]
    persons = row[DVPERSONS]
    expenditure = row[DVEXPEND]

    def compute_package():
        if package_cost == 0 and expenditure == 0 and befaf == 0:
            return 0, np.nan

        if (package_cost == 999999 or np.isnan(package_cost) or np.isnan(discounted_package_cost)
                or np.isnan(persons) or expenditure == 999999 or np.isnan(expenditure) or np.isnan(fare)
                or befaf == 999999 or np.isnan(befaf)):
            return np.nan, np.nan

        if ((discounted_package_cost + expenditure + befaf) / persons) < (fare * 2):
            return np.nan, 1

        return ((discounted_package_cost + expenditure + befaf) / persons) - (fare * 2), np.nan

    def compute_non_package():
        if package == 9:
            return np.nan

        if expenditure == 0 and befaf == 0:
            return 0

        if (expenditure == 999999 or np.isnan(expenditure)
                or befaf == 999999 or np.isnan(befaf) or np.isnan(persons)):
            return np.nan

        return (expenditure + befaf) / persons

    reason = np.nan

    if dv_package == 1:
        spend, reason = compute_package()
    else:
        spend = compute_non_package()

    if not np.isnan(spend) and not np.isnan(duty_free):
        spend = spend + duty_free

    # Ensure the spend values are integers
    spend = ips_rounding(spend, 0)

    row[SPEND] = spend
    row[SPENDIMPREASON] = reason

    return row