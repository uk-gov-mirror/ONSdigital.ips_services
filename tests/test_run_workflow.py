import time
import pandas as pd
import ips.persistence.sql as db
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

input_survey_data = 'data/import_data/dec/ips1712bv4_amtspnd.csv'
input_shift_data = 'data/calculations/december_2017/New_Dec_Data/Poss shifts Dec 2017.csv'
input_nr_data = 'data/calculations/december_2017/New_Dec_Data/Dec17_NR.csv'
input_unsampled_data = 'data/calculations/december_2017/New_Dec_Data/Unsampled Traffic Dec 2017.csv'
input_air_data = 'data/calculations/december_2017/New_Dec_Data/Air Sheet Dec 2017 VBA.csv'
input_sea_data = 'data/calculations/december_2017/New_Dec_Data/Sea Traffic Dec 2017.csv'
input_tunnel_data = 'data/calculations/december_2017/New_Dec_Data/Tunnel Traffic Dec 2017.csv'

survey_subsample_table = 'SURVEY_SUBSAMPLE'
run_id = 'el-is-the-best'
month = '12'
year = '2017'

start_time = time.time()

rail_fixed = False


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


def setup_pv():
    db.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)
    df = db.select_data('*', 'PROCESS_VARIABLE_PY', 'RUN_ID', 'TEMPLATE')
    df['RUN_ID'] = run_id
    db.insert_dataframe_into_table('PROCESS_VARIABLE_PY', df)


def teardown_module(module):
    log.info(f"Test duration: {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_time))}")


def test_shift_weight():
    log.info("Testing Calculation  1 --> shift_weight")
    shift_weight.shift_weight_step(run_id)


def test_non_response_weight():
    log.info("Testing Calculation  2 --> non_response_weight")
    non_response_weight.non_response_weight_step(run_id)


def test_minimums_weight():
    log.info("Testing Calculation  3 --> minimums_weight")
    minimums_weight.minimums_weight_step(run_id)


def test_traffic_weight():
    log.info("Testing Calculation  4 --> traffic_weight")
    traffic_weight.traffic_weight_step(run_id)


def test_unsampled_weight():
    log.info("Testing Calculation  5 --> unsampled_weight")
    unsampled_weight.unsampled_weight_step(run_id)


def test_imbalance_weight():
    log.info("Testing Calculation  6 --> imbalance_weight")
    imbalance_weight.imbalance_weight_step(run_id)


def test_final_weight():
    log.info("Testing Calculation  7 --> final_weight")
    final_weight.final_weight_step(run_id)


def test_stay_imputation():
    log.info("Testing Calculation  8 --> stay_imputation")
    stay_imputation.stay_imputation_step(run_id)


def test_fares_imputation():
    log.info("Testing Calculation  9 --> fares_imputation")
    fares_imputation.fares_imputation_step(run_id)


def test_spend_imputation():
    log.info("Testing Calculation 10 --> spend_imputation")
    spend_imputation.spend_imputation_step(run_id)


def test_rail_imputation():
    # Expected failure.  For further information see Confluence
    log.info("Testing Calculation 11 --> rail_imputation")
    rail_imputation.rail_imputation_step(run_id)


def test_regional_weight():
    # Expected failure due to inferred data types in CSV. See Confluence for further information.
    log.info("Testing Calculation 14 --> regional_weight")
    regional_weights.regional_weights_step(run_id)


def test_town_stay_expenditure_imputation():
    # Expected failure due to inferred data types in CSV. See Confluence for further information.
    log.info("Testing Calculation 13 --> town_stay_expenditure_imputation")
    town_stay_expenditure.town_stay_expenditure_imputation_step(run_id)


def test_airmiles():
    log.info("Testing Calculation 14 --> airiles")
    air_miles.airmiles_step(run_id)
