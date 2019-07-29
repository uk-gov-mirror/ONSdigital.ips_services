import time
import pandas as pd
import ips.persistence.sql as db
import zipfile
import shutil
import pytest
from pandas.testing import assert_frame_equal
from ips.util.services_logging import log
from ips.persistence.persistence import select_data
from ips.services.dataimport.import_survey import import_survey
from ips.services.dataimport.import_shift import import_shift
from ips.services.dataimport.import_non_response import import_nonresponse
from ips.services.dataimport.import_unsampled import import_unsampled
from ips.services.dataimport.import_traffic import import_air
from ips.services.dataimport.import_traffic import import_sea
from ips.services.dataimport.import_traffic import import_tunnel
from ips.services.steps import shift_weight, non_response_weight, minimums_weight, traffic_weight, unsampled_weight, \
    imbalance_weight, final_weight, stay_imputation, fares_imputation, spend_imputation, rail_imputation, \
    regional_weights, town_stay_expenditure, air_miles
from ips.util.services_configuration import ServicesConfiguration

from ips.persistence.persistence import execute_sql as exec_sql
execute_sql = exec_sql()

input_survey_data = 'data/import_data/quarter3/surveydata.csv'
input_shift_data = 'data/import_data/quarter3/Possible shifts Q3 2017.csv'
input_nr_data = 'data/import_data/quarter3/Non Response Q3 2017.csv'
input_unsampled_data = 'data/import_data/quarter3/Unsampled Traffic Q3 2017.csv'
input_air_data = 'data/import_data/quarter3/Air.csv'
input_sea_data = 'data/import_data/quarter3/Sea Traffic Q3 2017.csv'
input_tunnel_data = 'data/import_data/quarter3/Tunnel Traffic Q3 2017.csv'

survey_subsample_table = 'SURVEY_SUBSAMPLE'
run_id = 'h3re-1s-y0ur-run-1d-Q3'
month = 'Q3'
year = '2017'

start_time = time.time()

rail_fixed = False


def setup_module(module):
    unzip_data()

    # Load Survey data
    with open(input_survey_data, 'rb') as file:
        import_survey(run_id, file.read(), month, year)

    # Load Shift reference data
    with open(input_shift_data, 'rb') as file:
        import_shift(run_id, file.read(), month, year)

    # Load Non Response reference data
    with open(input_nr_data, 'rb') as file:
        import_nonresponse(run_id, file.read(), month, year)

    # Load Unsampled reference data
    with open(input_unsampled_data, 'rb') as file:
        import_unsampled(run_id, file.read(), month, year)

    # Load Air reference data
    with open(input_air_data, 'rb') as file:
        import_air(run_id, file.read(), month, year)

    # Load Sea reference data
    with open(input_sea_data, 'rb') as file:
        import_sea(run_id, file.read(), month, year)

    # Load Tunnel reference data
    with open(input_tunnel_data, 'rb') as file:
        import_tunnel(run_id, file.read(), month, year)

    log.info("Setting up PVs for testing")
    setup_pv()


def unzip_data():
    log.info("Unzipping import data")
    with zipfile.ZipFile("data/import_data/quarter3.zip", "r") as zip_ref:
        zip_ref.extractall("data/import_data")


def setup_pv():
    db.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
    df = db.select_data('*', 'PROCESS_VARIABLE_PY', 'RUN_ID', 'TEMPLATE')
    df['RUN_ID'] = run_id
    db.insert_dataframe_into_table('PROCESS_VARIABLE_PY', df)

    update_q3_pvs()


def update_q3_pvs():
    sql = f"""
    DELETE FROM PROCESS_VARIABLE_PY
    WHERE PV_NAME in ('samp_port_grp_pv', 'unsamp_port_grp_pv', 'unsamp_region_grp_pv')
    AND RUN_ID = '{run_id}'
    """

    execute_sql(sql)

    df = db.select_data('*', 'PROCESS_VARIABLE_PY', 'RUN_ID', 'Q3-TEST')
    df['RUN_ID'] = run_id
    db.insert_dataframe_into_table('PROCESS_VARIABLE_PY', df)


def teardown_module(module):
    log.info('Removing data/import_data/quarter3')
    shutil.rmtree('data/import_data/quarter3')
    log.info(f"Test duration: {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_time))}")


def test_shift_weight():
    log.info("Testing Calculation  1 --> shift_weight")
    shift_weight.shift_weight_step(run_id)

    survey_output(
        "SHIFT",
        "data/calculations/Q3_2017/shift_weight/surveydata_shiftq3.csv",
        [
            'SERIAL', 'SHIFT_WT'
        ]
    )

    summary_output(
        "SHIFT",
        "data/calculations/Q3_2017/shift_weight/shiftdata_summary2017.csv",
        "PS_SHIFT_DATA",
        [
            'SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV', 'MIGSI', 'POSS_SHIFT_CROSS',
            'SAMP_SHIFT_CROSS', 'MIN_SH_WT', 'MEAN_SH_WT', 'MAX_SH_WT', 'COUNT_RESPS', 'SUM_SH_WT'
        ]
    )


def test_non_response_weight():
    log.info("Testing Calculation  2 --> non_response_weight")
    non_response_weight.non_response_weight_step(run_id)

    survey_output(
        "NON_RESPONSE",
        "data/calculations/Q3_2017/non_response_weight/surveysubsample_nonresponse_q3.csv",
        [
            'SERIAL', 'NON_RESPONSE_WT'
        ]
    )

    summary_output(
        "NON_RESPONSE",
        "data/calculations/Q3_2017/non_response_weight/nr_summary_q32017.csv",
        "PS_NON_RESPONSE",
        [
            'NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT', 'COUNT_RESPS', 'PRIOR_SUM',
            'GROSS_RESP', 'GNR', 'MEAN_NR_WT'
        ]
    )


def test_minimums_weight():
    log.info("Testing Calculation  3 --> minimums_weight")
    minimums_weight.minimums_weight_step(run_id)

    survey_output(
        "MINIMUMS",
        "data/calculations/Q3_2017/min_weight/surveysubsample_mins_q3.csv",
        [
            'SERIAL', 'MINS_WT'
        ]
    )

    summary_output(
        "MINIMUMS",
        "data/calculations/Q3_2017/min_weight/mins_summary_q32017.csv",
        "PS_MINIMUMS",
        [
            'MINS_PORT_GRP_PV', 'ARRIVEDEPART', 'MINS_CTRY_GRP_PV', 'MINS_NAT_GRP_PV',
            'MINS_CTRY_PORT_GRP_PV', 'MINS_CASES', 'FULLS_CASES', 'PRIOR_GROSS_MINS',
            'PRIOR_GROSS_FULLS', 'PRIOR_GROSS_ALL', 'MINS_WT', 'POST_SUM', 'CASES_CARRIED_FWD'
        ]
    )


def test_traffic_weight():
    log.info("Testing Calculation  4 --> traffic_weight")
    traffic_weight.traffic_weight_step(run_id)
    survey_output(
        "TRAFFIC",
        "data/calculations/Q3_2017/traffic_weight/traffic_surveysubsample_2017.csv",
        [
            'SERIAL', 'TRAFFIC_WT'
        ]
    )

    summary_output(
        "TRAFFIC",
        "data/calculations/Q3_2017/traffic_weight/traffic_summary_q32017.csv",
        "PS_TRAFFIC",
        [
            'SAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'CASES', 'TRAFFICTOTAL', 'SUM_TRAFFIC_WT', 'TRAFFIC_WT'
        ]
    )


def test_unsampled_weight():
    log.info("Testing Calculation  5 --> unsampled_weight")
    unsampled_weight.unsampled_weight_step(run_id)

    survey_output(
        "UNSAMPLED",
        "data/calculations/Q3_2017/unsampled_weight/unsamp_surveysubsample_2017.csv",
        [
            'SERIAL', 'UNSAMP_TRAFFIC_WT'
        ]
    )

    summary_output(
        "UNSAMPLED",
        "data/calculations/Q3_2017/unsampled_weight/unsamp_summary_q32017.csv",
        "PS_UNSAMPLED_OOH",
        [
            'UNSAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'UNSAMP_REGION_GRP_PV', 'CASES', 'SUM_PRIOR_WT',
            'SUM_UNSAMP_TRAFFIC_WT', 'UNSAMP_TRAFFIC_WT'
        ]
    )


def test_imbalance_weight():
    log.info("Testing Calculation  6 --> imbalance_weight")
    imbalance_weight.imbalance_weight_step(run_id)
    survey_output(
        "IMBALANCE",
        "data/calculations/Q3_2017/imbalance_weight/imbalance_surveysubsample_2017.csv",
        [
            'SERIAL', 'IMBAL_WT'
        ]
    )

    summary_output(
        "IMBALANCE",
        "data/calculations/Q3_2017/imbalance_weight/imbalance_summary_q32017.csv",
        "PS_IMBALANCE",
        [
            'FLOW', 'SUM_PRIOR_WT', 'SUM_IMBAL_WT'
        ]
    )


def test_final_weight():
    log.info("Testing Calculation  7 --> final_weight")
    final_weight.final_weight_step(run_id)
    survey_output(
        "FINAL",
        "data/calculations/Q3_2017/final_weight/final_surveysubsample_2017.csv",
        [
            'SERIAL', 'FINAL_WT'
        ]
    )

    summary_output(
        "FINAL",
        "data/calculations/Q3_2017/final_weight/final_summary_q32017.csv",
        "PS_IMBALANCE",
        [
            'SERIAL', 'SHIFT_WT', 'NON_RESPONSE_WT', 'MINS_WT', 'TRAFFIC_WT',
            'UNSAMP_TRAFFIC_WT', 'IMBAL_WT', 'FINAL_WT'
        ]
    )


def test_stay_imputation():
    log.info("Testing Calculation  8 --> stay_imputation")
    stay_imputation.stay_imputation_step(run_id)
    survey_output(
        "STAY",
        "data/calculations/Q3_2017/stay/stay_surveysubsample_2017.csv",
        [
            'SERIAL', 'STAY', 'STAYK'
        ]
    )


def test_fares_imputation():
    log.info("Testing Calculation  9 --> fares_imputation")
    fares_imputation.fares_imputation_step(run_id)
    expected_failure = False

    # Assert failure when using Python's default bankers rounding. For further information see
    # https://collaborate2.ons.gov.uk/confluence/x/ArlfAQ
    conventional_rounding = ServicesConfiguration().sas_rounding()
    if not conventional_rounding:
        expected_failure = True

    survey_output(
        "FARES",
        "data/calculations/Q3_2017/fares/fares_surveysubsample_2017.csv",
        [
             'SERIAL', 'FARE', 'FAREK', 'SPEND', 'SPENDIMPREASON'
        ],
        expected_failure=expected_failure,
        cols_to_fail=['FARE', 'SPEND']
    )


def test_spend_imputation():
    log.info("Testing Calculation 10 --> spend_imputation")
    spend_imputation.spend_imputation_step(run_id)
    expected_failure = False

    # Assert failure when using the refactored PUR2_PV or Python's default bankers rounding. For further information see
    # https://collaborate2.ons.gov.uk/confluence/display/QSS/Spend+Imputation+Testing+Configuration and
    # https://collaborate2.ons.gov.uk/confluence/x/ArlfAQ
    refactor_pur2_pv = ServicesConfiguration().sas_pur2_pv()
    conventional_rounding = ServicesConfiguration().sas_rounding()

    if not refactor_pur2_pv or not conventional_rounding:
        expected_failure = True

    survey_output(
        "SPEND",
        "data/calculations/Q3_2017/spend/spend_surveysubsample_2017.csv",
        [
            'SERIAL', 'SPEND_IMP_FLAG_PV', 'SPEND_IMP_ELIGIBLE_PV', 'UK_OS_PV', 'PUR1_PV', 'PUR2_PV',
            'PUR3_PV', 'DUR1_PV', 'DUR2_PV', 'SPENDK', 'SPEND'
        ],
        expected_failure=expected_failure,
        cols_to_fail=['PUR2_PV', 'PUR3_PV', 'SPEND', 'SPENDK']
    )


def test_rail_imputation():
    # Expected failure.  For further information see: https://collaborate2.ons.gov.uk/confluence/x/ArlfAQ
    log.info("Testing Calculation 11 --> rail_imputation")
    rail_imputation.rail_imputation_step(run_id)


    survey_output(
        "RAIL",
        "data/calculations/Q3_2017/rail/rail_surveysubsample_2017.csv",
        [
            'SERIAL', 'RAIL_CNTRY_GRP_PV', 'RAIL_EXERCISE_PV', 'RAIL_IMP_ELIGIBLE_PV', 'SPEND'
        ],
        expected_failure=True,
        cols_to_fail=['SPEND']
    )


def test_regional_weight():
    # Expected failure.  For further information see: https://collaborate2.ons.gov.uk/confluence/x/ArlfAQ
    log.info("Testing Calculation 14 --> regional_weight")
    regional_weights.regional_weights_step(run_id)

    survey_output(
        "REGIONAL",
        'data/calculations/Q3_2017/regional_weights/regional_surveysubsample_2017.csv',
        [
            'SERIAL', 'VISIT_WT', 'VISIT_WTK', 'STAY_WT', 'STAY_WTK', 'EXPENDITURE_WT', 'EXPENDITURE_WTK', 'NIGHTS1',
            'NIGHTS2', 'NIGHTS3', 'NIGHTS4', 'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8', 'STAY1K', 'STAY2K', 'STAY3K',
            'STAY4K', 'STAY5K', 'STAY6K', 'STAY7K', 'STAY8K', 'PURPOSE_PV', 'STAYIMPCTRYLEVEL1_PV',
            'STAYIMPCTRYLEVEL2_PV', 'STAYIMPCTRYLEVEL3_PV', 'STAYIMPCTRYLEVEL4_PV', 'REG_IMP_ELIGIBLE_PV'
        ],
        expected_failure=True,
        cols_to_fail=['EXPENDITURE_WT', 'STAY1K', 'STAY2K', 'STAY3K', 'STAY4K', 'STAY5K', 'STAY6K', 'STAY7K', 'STAY8K', 'PURPOSE_PV',
                        'STAYIMPCTRYLEVEL1_PV', 'STAYIMPCTRYLEVEL2_PV', 'STAYIMPCTRYLEVEL3_PV',
                        'STAYIMPCTRYLEVEL4_PV', 'REG_IMP_ELIGIBLE_PV'
                      ]
    )


def test_town_stay_expenditure_imputation():
    # Expected failure.  For further information see: https://collaborate2.ons.gov.uk/confluence/x/ArlfAQ
    log.info("Testing Calculation 13 --> town_stay_expenditure_imputation")
    town_stay_expenditure.town_stay_expenditure_imputation_step(run_id)

    survey_output(
        "TOWN_AND_STAY",
        "data/calculations/Q3_2017/town_and_stay/town_surveysubsample_2017.csv",
        [
            'SERIAL', 'SPEND1', 'SPEND2', 'SPEND3', 'SPEND4', 'SPEND5', 'SPEND6', 'SPEND7', 'SPEND8', 'PURPOSE_PV',
            'STAYIMPCTRYLEVEL1_PV', 'STAYIMPCTRYLEVEL2_PV', 'STAYIMPCTRYLEVEL3_PV', 'STAYIMPCTRYLEVEL4_PV',
            'TOWN_IMP_ELIGIBLE_PV'
        ],
        expected_failure=True,
        cols_to_fail=['SPEND1', 'SPEND2', 'SPEND3', 'SPEND4', 'SPEND5', 'SPEND6', 'SPEND7', 'SPEND8', 'PURPOSE_PV',
                        'STAYIMPCTRYLEVEL1_PV', 'STAYIMPCTRYLEVEL2_PV', 'STAYIMPCTRYLEVEL3_PV',
                        'STAYIMPCTRYLEVEL4_PV', 'TOWN_IMP_ELIGIBLE_PV'
                      ]
    )


def test_airmiles():
    log.info("Testing Calculation 14 --> airiles")
    air_miles.airmiles_step(run_id)
    survey_output(
        "AIRMILES",
        "data/calculations/Q3_2017/air_miles/air_surveysubsample_2017.csv",
        [
            'SERIAL', 'UKLEG', 'OVLEG', 'DIRECTLEG'
        ]
    )


def survey_output(test_name, expected_survey_output, survey_output_columns, expected_failure=None, cols_to_fail=None):
    # Get survey results
    survey_subsample = select_data("*", survey_subsample_table, "RUN_ID", run_id)

    # Create comparison survey dataframes
    survey_results = survey_subsample[survey_output_columns].copy()
    survey_expected = pd.read_csv(expected_survey_output, engine='python')
    survey_expected = survey_expected[survey_output_columns].copy()

    # pandas.testing.faff
    survey_results.sort_values(by='SERIAL', axis=0, inplace=True)
    survey_results.index = range(0, len(survey_results))

    survey_expected.sort_values(by='SERIAL', axis=0, inplace=True)
    survey_expected.index = range(0, len(survey_expected))

    if expected_failure:
        assert_frame_not_equal(survey_results, survey_expected, cols_to_fail, test_name)

        # Drop failing columns and test remaining dataframe
        survey_results.drop(cols_to_fail, axis=1, inplace=True)
        survey_expected.drop(cols_to_fail, axis=1, inplace=True)

    # Test survey outputs
    log.info(f"Testing survey results for {test_name}")
    assert_frame_equal(survey_results, survey_expected, check_dtype=False, check_less_precise=True)


def summary_output(test_name, expected_summary_output, summary_output_table, summary_output_columns):
    # Get summary results
    log.info(f"Testing summary results for {test_name}")

    # Create comparison summary dataframes
    summary_expected = pd.read_csv(expected_summary_output, engine='python')

    if test_name != 'FINAL':
        # Summary data is exported from table
        summary_data = select_data("*", summary_output_table, "RUN_ID", run_id)
        summary_results = summary_data.copy()
        summary_results.drop('RUN_ID', axis=1, inplace=True)
    else:
        # Final Weight Summary data is a subsample of records from the Survey output
        survey_subsample = select_data("*", survey_subsample_table, "RUN_ID", run_id)
        summary_data = survey_subsample[survey_subsample['SERIAL'].isin(summary_expected['SERIAL'])]
        summary_results = summary_data[summary_output_columns].copy()

    # pandas testing faff
    summary_results.sort_values(by=summary_output_columns, axis=0, inplace=True)
    summary_expected.sort_values(by=summary_output_columns, axis=0, inplace=True)
    summary_results.index = range(0, len(summary_results))
    summary_expected.index = range(0, len(summary_expected))

    # Test summary outputs
    assert_frame_equal(summary_results, summary_expected, check_dtype=False, check_less_precise=True)


def assert_frame_not_equal(results, expected, columns, test_name):
    # Mismatching dataframes will result in a positive result
    for col in columns:
        python_df = results[['SERIAL', col]]
        sas_df = expected[['SERIAL', col]]

        try:
            log.info(f"Asserting {col} not equal for {test_name}")
            assert_frame_equal(python_df, sas_df)
        except AssertionError:
            # frames are not equal
            log.info(f"{col} does not match SAS output:  Expected behaviour")
            return True
        else:
            # frames are equal
            log.warning(f"{col} matches SAS output: Unexpected behaviour.")
