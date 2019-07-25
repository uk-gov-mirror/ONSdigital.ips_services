from pandas.testing import assert_frame_equal
from ips.persistence import data_management as idm
from ips.persistence.persistence import insert_from_dataframe, truncate_table
from ips.util.services_configuration import ServicesConfiguration
from ips.persistence.persistence import select_data
from ips.util.services_logging import log
from ips.services.calculations import log_warnings
import numpy as np
import pandas as pd

output_columns = []

survey_in = ''
expected_data = ''


def df_diff(expected, result):
    df_bool = (expected != result).stack()
    diff = pd.concat([expected.stack()[df_bool], result.stack()[df_bool]], axis=1)
    diff.columns = ["Expected", "Result"]
    return diff


def test_step():
    config = ServicesConfiguration().get_regional_weights()
    run_id = 'h3re-1s-y0ur-run-1d-Q3'

    # Load survey data
    survey_data = pd.read_csv(survey_in, engine='python')
    survey_data.reset_index(drop=True, inplace=True)

    truncate_table("SAS_SURVEY_SUBSAMPLE")()
    insert_from_dataframe('SAS_SURVEY_SUBSAMPLE')(survey_data)

    # Run calculation and subsequent db steps
    survey_data_out = do_ips_regional_weight_calculation(
        survey_data,
        serial_num='SERIAL',
        final_weight='FINAL_WT'
    )

    insert_from_dataframe(config["temp_table"])(survey_data_out)
    idm.update_survey_data_with_step_results(config)
    idm.store_survey_data_with_step_results(run_id, config)

    # Start testing shizznizz
    survey_subsample = select_data("*", 'SURVEY_SUBSAMPLE', "RUN_ID", run_id)
    survey_results = survey_subsample[output_columns].copy()

    survey_expected = pd.read_csv(expected_data, engine='python')
    survey_expected = survey_expected[output_columns].copy()

    # pandas.testing.faff
    survey_expected['SERIAL'] = survey_expected['SERIAL'].astype(int)
    survey_results['SERIAL'] = survey_results['SERIAL'].astype(int)
    survey_expected.fillna(0, inplace=True)
    survey_results.fillna(0, inplace=True)

    survey_expected.set_index('SERIAL', inplace=True)
    survey_expected.sort_index(inplace=True)
    survey_results.set_index('SERIAL', inplace=True)
    survey_results.sort_index(inplace=True)

    # survey_expected.index = range(0, len(survey_expected))
    # survey_results.index = range(0, len(survey_results))

    diff = df_diff(survey_expected, survey_results)
    print()
    print("# of different rows: " + str(len(diff.index)))
    print()
    print(diff)

    assert_frame_equal(survey_expected, survey_results, check_dtype=False, check_less_precise=True)


####################

