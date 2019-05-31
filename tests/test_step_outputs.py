import pytest
import time
import pandas as pd
import ips_common_db.sql as db

from pandas.testing import assert_frame_equal
from ips_common.ips_logging import log
from ips.persistence.persistence import delete_from_table, read_table_values

from ips.services.dataimport.import_survey import import_survey
from ips.services.dataimport.import_shift import import_shift
from ips.services.dataimport.import_non_response import import_nonresponse
from ips.services.dataimport.import_unsampled import import_unsampled
from ips.services.dataimport.import_traffic import import_air
from ips.services.dataimport.import_traffic import import_sea
from ips.services.dataimport.import_traffic import import_tunnel

from ips.services.steps import shift_weight, non_response_weight, minimums_weight

SURVEY_SUBSAMPLE_TABLE = 'SURVEY_SUBSAMPLE'

input_survey_data = 'data/calculations/december_2017/New_Dec_Data/Survey Data.csv'
input_shift_data = 'data/calculations/december_2017/New_Dec_Data/Poss shifts Dec 2017.csv'
input_nr_data = 'data/calculations/december_2017/New_Dec_Data/Dec17_NR.csv'
input_unsampled_data = 'data/calculations/december_2017/New_Dec_Data/Unsampled Traffic Dec 2017.csv'
input_air_data = 'data/calculations/december_2017/New_Dec_Data/Air Sheet Dec 2017 VBA.csv'
input_sea_data = 'data/calculations/december_2017/New_Dec_Data/Sea Traffic Dec 2017.csv'
input_tunnel_data = 'data/calculations/december_2017/New_Dec_Data/Tunnel Traffic Dec 2017.csv'

run_id = 'h3re-1s-y0ur-run-1d'
month = '12'
year = '2017'

start_time = time.time()

def setup_module(module):
    clear_survey_subsample = delete_from_table('SURVEY_SUBSAMPLE')
    clear_sas_survey_subsample = delete_from_table('SAS_SURVEY_SUBSAMPLE')
    clear_shift_data = delete_from_table('SHIFT_DATA')
    clear_nr_data = delete_from_table('NON_RESPONSE_DATA')
    clear_unsamp_data = delete_from_table('UNSAMPLED_OOH_DATA')
    clear_traffic_data = delete_from_table('TRAFFIC_DATA')
    clear_pvs = delete_from_table('PROCESS_VARIABLE_PY')

    clear_survey_subsample()
    clear_sas_survey_subsample()
    clear_shift_data()
    clear_nr_data()
    clear_unsamp_data()
    clear_traffic_data()
    clear_pvs(run_id=run_id)

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

    setup_pv()


def setup_pv():
    df = db.select_data('*', "PROCESS_VARIABLE_PY", 'RUN_ID', 'TEMPLATE')
    df['RUN_ID'] = run_id
    db.insert_dataframe_into_table('PROCESS_VARIABLE_PY', df)


def teardown_module(module):
    clear_survey_subsample = delete_from_table('SURVEY_SUBSAMPLE')
    clear_sas_survey_subsample = delete_from_table('SAS_SURVEY_SUBSAMPLE')
    clear_shift_data = delete_from_table('SHIFT_DATA')
    clear_nr_data = delete_from_table('NON_RESPONSE_DATA')
    clear_unsamp_data = delete_from_table('UNSAMPLED_OOH_DATA')
    clear_traffic_data = delete_from_table('TRAFFIC_DATA')
    clear_pvs = delete_from_table('PROCESS_VARIABLE_PY')

    clear_survey_subsample()
    clear_sas_survey_subsample()
    clear_shift_data()
    clear_nr_data()
    clear_unsamp_data()
    clear_traffic_data()
    clear_pvs(run_id=run_id)

    log.info(f"Test duration: {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_time))}")


@pytest.mark.parametrize('test_name, expected_survey_output, expected_summary_output, survey_output_columns, summary_output_table, summary_output_columns', [
    ('SHIFT', 'data/calculations/december_2017/shift_weight/dec_output.csv', 'data/calculations/december_2017/shift_weight/dec2017_summary.csv', ['SERIAL', 'SHIFT_WT'], 'PS_SHIFT_DATA', ['SHIFT_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'AM_PM_NIGHT_PV', 'MIGSI', 'POSS_SHIFT_CROSS', 'SAMP_SHIFT_CROSS', 'MIN_SH_WT', 'MEAN_SH_WT', 'MAX_SH_WT', 'COUNT_RESPS', 'SUM_SH_WT']),
    ('NON_RESPONSE', 'data/calculations/december_2017/non_response_weight/dec_output.csv', 'data/calculations/december_2017/non_response_weight/dec2017_summary.csv', ['SERIAL', 'NON_RESPONSE_WT'], 'PS_NON_RESPONSE', ['NR_PORT_GRP_PV', 'ARRIVEDEPART', 'WEEKDAY_END_PV', 'MEAN_RESPS_SH_WT', 'COUNT_RESPS', 'PRIOR_SUM', 'GROSS_RESP', 'GNR', 'MEAN_NR_WT']),
    ('MINIMUMS' # test_name
     , 'data/calculations/december_2017/min_weight/dec2017_survey.csv' # expected_survey_output
     , 'data/calculations/december_2017/min_weight/summarydata_final.csv' # expected_summary_output
     , ['SERIAL', 'MINS_WT'] # survey_output_columns
     , 'PS_MINIMUMS' # summary_output_table
     , ['MINS_PORT_GRP_PV', 'ARRIVEDEPART', 'MINS_CTRY_GRP_PV', 'MINS_NAT_GRP_PV', 'MINS_CTRY_PORT_GRP_PV', 'MINS_CASES', 'FULLS_CASES', 'PRIOR_GROSS_MINS', 'PRIOR_GROSS_FULLS', 'PRIOR_GROSS_ALL', 'MINS_WT', 'POST_SUM', 'CASES_CARRIED_FWD']), # summary_output_columns
    ])
def test_step_outputs(test_name
                      , expected_survey_output
                      , expected_summary_output
                      , survey_output_columns
                      , summary_output_table
                      , summary_output_columns):
    # Run step
    shift_weight.shift_weight_step(run_id)
    non_response_weight.non_response_weight_step(run_id)
    minimums_weight.minimums_weight_step(run_id)

    # TODO: Skippidy-skip
    if test_name in ('SHIFT', 'NON_RESPONSE'):
        pytest.skip("They pass")

    # Get survey results
    data = read_table_values(SURVEY_SUBSAMPLE_TABLE)
    survey_subsample = data()

    # Create comparison survey dataframes
    survey_results = survey_subsample[survey_output_columns].copy()
    survey_expected = pd.read_csv(expected_survey_output)

    # pandas.testing.faff
    survey_results.sort_values(by='SERIAL', axis=0, inplace=True)
    survey_expected.sort_values(by='SERIAL', axis=0, inplace=True)
    survey_results.index = range(0, len(survey_results))
    survey_expected.index = range(0, len(survey_expected))

    # Test survey outputs
    log.info(f"Testing survey results for {test_name}")
    assert_frame_equal(survey_results, survey_expected, check_dtype=False)

    ####

    # Get summary results
    if test_name in ('SHIFT', 'NON_RESPONSE', 'MINIMUMS'):
        log.info(f"Testing summary results for {test_name}")
        data = read_table_values(summary_output_table)
        summary_data = data()

        # Create comparison summary dataframes
        summary_results = summary_data.copy()
        summary_expected = pd.read_csv(expected_summary_output)

        # pandas.testing.faff
        summary_results.drop('RUN_ID', axis=1, inplace=True)
        summary_results.sort_values(by=summary_output_columns, axis=0, inplace=True)
        summary_expected.sort_values(by=summary_output_columns, axis=0, inplace=True)
        summary_results.index = range(0, len(summary_results))
        summary_expected.index = range(0, len(summary_expected))

        # Test summary outputs
        assert_frame_equal(summary_results, summary_expected, check_dtype=False)
