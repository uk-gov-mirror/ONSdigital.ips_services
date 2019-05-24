import pytest
import time
import pandas as pd
import ips_common_db.sql as db

from pandas.testing import assert_frame_equal
from ips_common.ips_logging import log
from ips.services.dataimport.import_survey import import_survey
from ips.services.dataimport.import_shift import import_shift
from ips.services.dataimport.import_non_response import import_nonresponse
from ips.persistence.persistence import delete_from_table
from ips.services.steps import shift_weight
from ips.services.steps import non_response_weight
from ips.persistence.persistence import read_table_values

SURVEY_SUBSAMPLE_TABLE = 'SURVEY_SUBSAMPLE'

input_survey_data = 'data/import_data/dec/ips1712bv4_amtspnd.csv'
input_shift_data = 'data/import_data/dec/Poss shifts Dec 2017.csv'
input_nr_data = 'data/import_data/dec/Dec17_NR.csv'

run_id = 'h3re-1s-y0ur-run-1d'
month = '12'
year = '2017'

start_time = time.time()

def setup_module(module):
    # Load Survey data
    with open(input_survey_data, 'rb') as file:
        import_survey(run_id, file.read(), month, year)

    # Load Shift reference data
    with open(input_shift_data, 'rb') as file:
        import_shift(run_id, file.read(), month, year)

    # Load Non Response reference data
    with open(input_nr_data, 'rb') as file:
        import_nonresponse(run_id, file.read(), month, year)

    setup_pv()


def setup_pv():
    df = db.select_data('*', "PROCESS_VARIABLE_PY", 'RUN_ID', 'TEMPLATE')
    df['RUN_ID'] = run_id
    db.insert_dataframe_into_table('PROCESS_VARIABLE_PY', df)


def teardown_module(module):
    clear_survey_subsample = delete_from_table('SURVEY_SUBSAMPLE')
    clear_shift_data = delete_from_table('SHIFT_DATA')
    clear_nr_data = delete_from_table('NON_RESPONSE_DATA')

    clear_survey_subsample()
    clear_shift_data()
    clear_nr_data()

    log.info(f"Test duration: {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_time))}")


@pytest.mark.parametrize('expected_survey_output, expected_summary_output, survey_output_columns, summary_output_table, summary_output_columns', [
    ('data/calculations/december_2017/shift_weight/dec_output.csv', 'data/calculations/december_2017/shift_weight/dec2017_summary.csv', ['SERIAL', 'SHIFT_WT'], 'SHIFT_DATA', ['PORTROUTE', 'WEEKDAY', 'ARRIVEDEPART', 'TOTAL', 'AM_PM_NIGHT']),
    ('data/calculations/december_2017/non_response_weight/dec_output.csv', 'data/calculations/december_2017/non_response_weight/dec2017_summary.csv', ['SERIAL', 'NON_RESPONSE_WT'], 'NON_RESPONSE_DATA', ['PORTROUTE', 'WEEKDAY', 'ARRIVEDEPART', 'AM_PM_NIGHT', 'SAMPINTERVAL', 'MIGTOTAL', 'ORDTOTAL']),
    ])
def test_step_outputs(expected_survey_output, expected_summary_output, survey_output_columns, summary_output_table, summary_output_columns):
    # Run step
    shift_weight.shift_weight_step(run_id)
    non_response_weight.non_response_weight_step(run_id)

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
    assert_frame_equal(survey_results, survey_expected, check_dtype=False)

    ####

    # Get summary results
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
