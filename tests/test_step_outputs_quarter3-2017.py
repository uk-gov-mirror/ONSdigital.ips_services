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

    update_traffic_weight_pv()


def update_traffic_weight_pv():
    samp_port_grp_pv = """
    '
    if row[''PORTROUTE''] in (111, 113, 119, 161, 171):
        row[''SAMP_PORT_GRP_PV''] = ''A111''
    elif row[''PORTROUTE''] in (121, 123, 162, 172):
        row[''SAMP_PORT_GRP_PV''] = ''A121''
    elif row[''PORTROUTE''] in (131, 132, 133, 134, 135, 163, 173):
        row[''SAMP_PORT_GRP_PV''] = ''A131''
    elif row[''PORTROUTE''] in (141, 142, 143, 144, 145, 164, 174):
        row[''SAMP_PORT_GRP_PV''] = ''A141''
    elif row[''PORTROUTE''] in (151, 152, 153, 154, 165, 175):
        row[''SAMP_PORT_GRP_PV''] = ''A151''
    elif row[''PORTROUTE''] in (181, 183, 189):
        row[''SAMP_PORT_GRP_PV''] = ''A181''
    elif row[''PORTROUTE''] in (191, 192, 193, 199):
        row[''SAMP_PORT_GRP_PV''] = ''A191''
    elif row[''PORTROUTE''] in (201, 202, 203, 204):
        row[''SAMP_PORT_GRP_PV''] = ''A201''
    elif row[''PORTROUTE''] in (211, 213, 219):
        row[''SAMP_PORT_GRP_PV''] = ''A211''
    elif row[''PORTROUTE''] in (221, 223):
        row[''SAMP_PORT_GRP_PV''] = ''A221''
    elif row[''PORTROUTE''] in (231, 232, 234):
        row[''SAMP_PORT_GRP_PV''] = ''A231''
    elif row[''PORTROUTE''] in (241, 243, 249):
        row[''SAMP_PORT_GRP_PV''] = ''A241''
    elif row[''PORTROUTE''] in (311, 313, 319):
        row[''SAMP_PORT_GRP_PV''] = ''A311''
    elif row[''PORTROUTE''] == 321:
        row[''SAMP_PORT_GRP_PV''] = ''A321''
    elif row[''PORTROUTE''] == 331:
        row[''SAMP_PORT_GRP_PV''] = ''A331''
    elif row[''PORTROUTE''] == 351:
        row[''SAMP_PORT_GRP_PV''] = ''A351''
    elif row[''PORTROUTE''] == 361:
        row[''SAMP_PORT_GRP_PV''] = ''A361''
    elif row[''PORTROUTE''] == 371:
        row[''SAMP_PORT_GRP_PV''] = ''A371''
    elif row[''PORTROUTE''] in (381, 382):
        row[''SAMP_PORT_GRP_PV''] = ''A381''
    elif row[''PORTROUTE''] in (341, 391, 393):
        row[''SAMP_PORT_GRP_PV''] = ''A391''
    elif row[''PORTROUTE''] == 401:
        row[''SAMP_PORT_GRP_PV''] = ''A401''
    elif row[''PORTROUTE''] == 411:
        row[''SAMP_PORT_GRP_PV''] = ''A411''
    elif row[''PORTROUTE''] in (421, 423):
        row[''SAMP_PORT_GRP_PV''] = ''A421''
    elif row[''PORTROUTE''] in (441, 443):
        row[''SAMP_PORT_GRP_PV''] = ''A441''
    elif row[''PORTROUTE''] == 451:
        row[''SAMP_PORT_GRP_PV''] = ''A451''
    elif row[''PORTROUTE''] == 461:
        row[''SAMP_PORT_GRP_PV''] = ''A461''
    elif row[''PORTROUTE''] == 471:
        row[''SAMP_PORT_GRP_PV''] = ''A471''
    elif row[''PORTROUTE''] == 481:
        row[''SAMP_PORT_GRP_PV''] = ''A481''
    elif row[''PORTROUTE''] in (611, 612, 613):
        row[''SAMP_PORT_GRP_PV''] = ''DCF''
    elif row[''PORTROUTE''] in (621, 631, 632, 633, 634, 651, 652, 662):
        row[''SAMP_PORT_GRP_PV''] = ''SCF''
    elif row[''PORTROUTE''] == 641:
        row[''SAMP_PORT_GRP_PV''] = ''LHS''
    elif row[''PORTROUTE''] in (635, 636, 661):
        row[''SAMP_PORT_GRP_PV''] = ''SLR''
    elif row[''PORTROUTE''] == 671:
        row[''SAMP_PORT_GRP_PV''] = ''HBN''
    elif row[''PORTROUTE''] == 672:
        row[''SAMP_PORT_GRP_PV''] = ''HGS''
    elif row[''PORTROUTE''] == 681:
        row[''SAMP_PORT_GRP_PV''] = ''EGS''
    elif row[''PORTROUTE''] in (701, 711, 741):
        row[''SAMP_PORT_GRP_PV''] = ''SSE''
    elif row[''PORTROUTE''] in (721, 722):
        row[''SAMP_PORT_GRP_PV''] = ''SNE''
    elif row[''PORTROUTE''] in (731, 682, 691, 692):
        row[''SAMP_PORT_GRP_PV''] = ''RSS''
    elif row[''PORTROUTE''] in (811, 813):
        row[''SAMP_PORT_GRP_PV''] = ''T811''
    elif row[''PORTROUTE''] == 812:
        row[''SAMP_PORT_GRP_PV''] = ''T811''
    elif row[''PORTROUTE''] in (911, 913):
        row[''SAMP_PORT_GRP_PV''] = ''E911''
    elif row[''PORTROUTE''] == 921:
        row[''SAMP_PORT_GRP_PV''] = ''E921''
    elif row[''PORTROUTE''] == 951:
        row[''SAMP_PORT_GRP_PV''] = ''E951''
    
    Irish = 0
    IoM = 0
    ChannelI = 0
    dvpc = 0
    
    if dataset == ''survey'':
        if not math.isnan(row[''DVPORTCODE'']):
            dvpc = int(row[''DVPORTCODE''] / 1000)
    
        if dvpc == 372:
            Irish = 1
        elif row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):
            if ((row[''FLOW''] in (1, 3)) and (row[''RESIDENCE''] == 372)):
                Irish = 1
            elif ((row[''FLOW''] in (2, 4)) and (row[''COUNTRYVISIT''] == 372)):
                Irish = 1
    
        if dvpc == 833:
            IoM = 1
        elif row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):
            if ((row[''FLOW''] in (1, 3)) and (row[''RESIDENCE''] == 833)):
                IoM = 1
            elif ((row[''FLOW''] in (2, 4)) and (row[''COUNTRYVISIT''] == 833)):
                IoM = 1
    
        if dvpc in (831, 832, 931):
            ChannelI = 1
        elif row[''DVPORTCODE''] == 999999 or math.isnan(row[''DVPORTCODE'']):
            if ((row[''FLOW''] in (1, 3)) and (row[''RESIDENCE''] in (831, 832, 931))):
                ChannelI = 1
            elif ((row[''FLOW''] in (2, 4)) and (row[''COUNTRYVISIT''] in (831, 832, 931))):
                ChannelI = 1
    elif dataset == ''traffic'':
        if row[''HAUL''] == ''E'':
            Irish = 1
        elif (row[''PORTROUTE''] == 250) or (row[''PORTROUTE''] == 350):
            ChannelI = 1
        elif (row[''PORTROUTE''] == 260) or (row[''PORTROUTE''] == 360):
            IoM = 1
    
    if (Irish) and row[''PORTROUTE''] in (
    111, 121, 131, 141, 132, 142, 119, 161, 162, 163, 164, 165, 151, 152, 171, 173, 174, 175):
        row[''SAMP_PORT_GRP_PV''] = ''AHE''
    elif (Irish) and row[''PORTROUTE''] in (181, 191, 192, 189, 199):
        row[''SAMP_PORT_GRP_PV''] = ''AGE''
    elif (Irish) and row[''PORTROUTE''] in (211, 221, 231, 219):
        row[''SAMP_PORT_GRP_PV''] = ''AME''
    elif (Irish) and row[''PORTROUTE''] in (241, 249):
        row[''SAMP_PORT_GRP_PV''] = ''ALE''
    elif (Irish) and row[''PORTROUTE''] in (201, 202):
        row[''SAMP_PORT_GRP_PV''] = ''ASE''
    elif (Irish) and (row[''PORTROUTE''] >= 300) and (row[''PORTROUTE''] < 600):
        row[''SAMP_PORT_GRP_PV''] = ''ARE''
    elif (ChannelI) and (row[''PORTROUTE''] >= 100) and (row[''PORTROUTE''] < 300):
        row[''SAMP_PORT_GRP_PV''] = ''MAC''
    elif (ChannelI) and (row[''PORTROUTE''] >= 300) and (row[''PORTROUTE''] < 600):
        row[''SAMP_PORT_GRP_PV''] = ''RAC''
    elif (IoM) and (row[''PORTROUTE''] >= 100) and (row[''PORTROUTE''] < 300):
        row[''SAMP_PORT_GRP_PV''] = ''MAM''
    elif (IoM) and (row[''PORTROUTE''] >= 300) and (row[''PORTROUTE''] < 600):
        row[''SAMP_PORT_GRP_PV''] = ''RAM''
    
    if row[''SAMP_PORT_GRP_PV''] == ''HGS'':
        row[''SAMP_PORT_GRP_PV''] = ''HBN''
    
    if row[''SAMP_PORT_GRP_PV''] == ''EGS'':
        row[''SAMP_PORT_GRP_PV''] = ''HBN''
    
    if row[''SAMP_PORT_GRP_PV''] == ''MAM'':
        row[''SAMP_PORT_GRP_PV''] = ''MAC''
    
    if row[''SAMP_PORT_GRP_PV''] == ''RAM'':
        row[''SAMP_PORT_GRP_PV''] = ''RAC''
    
    if row[''SAMP_PORT_GRP_PV''] == ''A331'' and (row[''ARRIVEDEPART''] == 1):
        row[''SAMP_PORT_GRP_PV''] = ''A391''
    if row[''SAMP_PORT_GRP_PV''] == ''A331'' and (row[''ARRIVEDEPART''] == 2):
        row[''SAMP_PORT_GRP_PV''] = ''A391''
    
    if row[''SAMP_PORT_GRP_PV''] == ''A401'' and (row[''ARRIVEDEPART''] == 1):
        row[''SAMP_PORT_GRP_PV''] = ''A441''
    if row[''SAMP_PORT_GRP_PV''] == ''A401'' and (row[''ARRIVEDEPART''] == 2):
        row[''SAMP_PORT_GRP_PV''] = ''A441''
    
    if row[''SAMP_PORT_GRP_PV''] == ''SLR'' and (row[''ARRIVEDEPART''] == 1):
        row[''SAMP_PORT_GRP_PV''] = ''SCF''
    if row[''SAMP_PORT_GRP_PV''] == ''SLR'' and (row[''ARRIVEDEPART''] == 2):
        row[''SAMP_PORT_GRP_PV''] = ''SCF''
    
    if row[''SAMP_PORT_GRP_PV''] == ''SSE'' and (row[''ARRIVEDEPART''] == 1):
        row[''SAMP_PORT_GRP_PV''] = ''SNE''
    
    if row[''SAMP_PORT_GRP_PV''] == ''LHS'':
        row[''SAMP_PORT_GRP_PV''] = ''HBN''
    '
    """

    sql = f"""
    UPDATE PROCESS_VARIABLE_PY
    SET PV_DEF = {samp_port_grp_pv}
    WHERE PV_NAME = 'SAMP_PORT_GRP_PV'
    AND RUN_ID = '{run_id}'
    """

    execute_sql(sql)


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

             
# def test_traffic_weight():
#     log.info("Testing Calculation  4 --> traffic_weight")
#     traffic_weight.traffic_weight_step(run_id)
#     survey_output(
#         "TRAFFIC",
#         "data/calculations/Q3_2017/traffic_weight/traffic_surveysubsample_2017.csv",
#         [
#             'SERIAL', 'TRAFFIC_WT'
#         ]
#     )
#
#     summary_output(
#         "TRAFFIC",
#         "data/calculations/Q3_2017/traffic_weight/traffic_summary_q32017.csv",
#         "PS_TRAFFIC",
#         [
#             'SAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'CASES', 'TRAFFICTOTAL', 'SUM_TRAFFIC_WT', 'TRAFFIC_WT'
#         ]
#     )
#
#
# def test_unsampled_weight():
#     log.info("Testing Calculation  5 --> unsampled_weight")
#     unsampled_weight.unsampled_weight_step(run_id)
#     survey_output(
#         "UNSAMPLED",
#         "data/calculations/Q3_2017/unsampled_weight/unsamp_surveysubsample_2017.csv",
#         [
#             'SERIAL', 'UNSAMP_TRAFFIC_WT'
#         ]
#     )
#
#     summary_output(
#         "UNSAMPLED",
#         "data/calculations/Q3_2017/unsampled_weight/unsamp_summary_q32017.csv",
#         "PS_UNSAMPLED_OOH",
#         [
#             'UNSAMP_PORT_GRP_PV', 'ARRIVEDEPART', 'UNSAMP_REGION_GRP_PV', 'CASES', 'SUM_PRIOR_WT',
#             'SUM_UNSAMP_TRAFFIC_WT', 'UNSAMP_TRAFFIC_WT'
#         ]
#     )
#
#
# def test_imbalance_weight():
#     log.info("Testing Calculation  6 --> imbalance_weight")
#     imbalance_weight.imbalance_weight_step(run_id)
#     survey_output(
#         "IMBALANCE",
#         "data/calculations/Q3_2017/imbalance_weight/imbalance_surveysubsample_2017.csv",
#         [
#             'SERIAL', 'IMBAL_WT'
#         ]
#     )
#
#     summary_output(
#         "IMBALANCE",
#         "data/calculations/Q3_2017/imbalance_weight/imbalance_summary_q32017.csv",
#         "PS_IMBALANCE",
#         [
#             'FLOW', 'SUM_PRIOR_WT', 'SUM_IMBAL_WT'
#         ]
#     )
#
#
# def test_final_weight():
#     log.info("Testing Calculation  7 --> final_weight")
#     final_weight.final_weight_step(run_id)
#     survey_output(
#         "FINAL",
#         "data/calculations/Q3_2017/final_weight/final_surveysubsample_2017.csv",
#         [
#             'SERIAL', 'FINAL_WT'
#         ]
#     )
#
#     summary_output(
#         "FINAL",
#         "data/calculations/Q3_2017/final_weight/final_summary_q32017.csv",
#         "PS_IMBALANCE",
#         [
#             'SERIAL', 'SHIFT_WT', 'NON_RESPONSE_WT', 'MINS_WT', 'TRAFFIC_WT',
#             'UNSAMP_TRAFFIC_WT', 'IMBAL_WT', 'FINAL_WT'
#         ]
#     )
#
#
# def test_stay_imputation():
#     log.info("Testing Calculation  8 --> stay_imputation")
#     stay_imputation.stay_imputation_step(run_id)
#     survey_output(
#         "STAY",
#         "data/calculations/Q3_2017/fares/stay_surveysubsample_2017.csv",
#         [
#             'SERIAL', 'STAY', 'STAYK'
#         ]
#     )
#
#
# def test_fares_imputation():
#     log.info("Testing Calculation  9 --> fares_imputation")
#     fares_imputation.fares_imputation_step(run_id)
#     survey_output(
#         "FARES",
#         "data/calculations/Q3_2017/fares/fares_surveysubsample_2017.csv",
#         [
#              'SERIAL', 'FARE', 'FAREK', 'SPEND', 'SPENDIMPREASON'
#         ]
#     )
#
#
# def test_spend_imputation():
#     log.info("Testing Calculation 10 --> spend_imputation")
#     spend_imputation.spend_imputation_step(run_id)
#
#     # Assert 'PUR2_PV', 'SPEND' and 'SPENDK' columns are not equal. For further information see
#     # https://collaborate2.ons.gov.uk/confluence/display/QSS/Spend+Imputation+Testing+Configuration
#     status = ServicesConfiguration().sas_pur2_pv()
#
#     survey_output(
#         "SPEND",
#         "data/calculations/Q3_2017/spend/spend_surveysubsample_2017.csv",
#         [
#             'SERIAL', 'SPEND_IMP_FLAG_PV', 'SPEND_IMP_ELIGIBLE_PV', 'UK_OS_PV', 'PUR1_PV', 'PUR2_PV',
#             'PUR3_PV', 'DUR1_PV', 'DUR2_PV', 'SPENDK', 'SPEND'
#         ],
#         status=status,
#         false_cols=['PUR2_PV', 'SPEND', 'SPENDK']
#     )
#
#
# # @pytest.mark.xfail()
# def test_rail_imputation():
#     # Expected failure on SPEND column.  For further information see:
#     # https://collaborate2.ons.gov.uk/confluence/display/QSS/Differing+values+between+SAS+and+Python+outputs
#     log.info("Testing Calculation 11 --> rail_imputation")
#     rail_imputation.rail_imputation_step(run_id)
#
#     survey_output(
#         "RAIL",
#         "data/calculations/Q3_2017/rail/rail_surveysubsample_2017.csvv",
#         [
#             'SERIAL', 'RAIL_CNTRY_GRP_PV', 'RAIL_EXERCISE_PV', 'RAIL_IMP_ELIGIBLE_PV', 'SPEND'
#         ],
#     )
#
#
# # @pytest.mark.xfail()
# def test_regional_weight():
#     # Expected failure on VISIT_WT and EXPENDITURE_WT columns.  For further information see:
#     # https://collaborate2.ons.gov.uk/confluence/display/QSS/Differing+values+between+SAS+and+Python+outputs
#
#     log.info("Testing Calculation 14 --> regional_weight")
#     regional_weights.regional_weights_step(run_id)
#
#     survey_output(
#         "REGIONAL",
#         'data/calculations/Q3_2017/regional_weights/regional_surveysubsample_2017.csv',
#         [
#             'SERIAL', 'VISIT_WT', 'VISIT_WTK', 'STAY_WT', 'STAY_WTK', 'EXPENDITURE_WT', 'EXPENDITURE_WTK', 'NIGHTS1',
#             'NIGHTS2', 'NIGHTS3', 'NIGHTS4', 'NIGHTS5', 'NIGHTS6', 'NIGHTS7', 'NIGHTS8', 'STAY1K', 'STAY2K', 'STAY3K',
#             'STAY4K', 'STAY5K', 'STAY6K', 'STAY7K', 'STAY8K', 'PURPOSE_PV', 'STAYIMPCTRYLEVEL1_PV',
#             'STAYIMPCTRYLEVEL2_PV', 'STAYIMPCTRYLEVEL3_PV', 'STAYIMPCTRYLEVEL4_PV', 'REG_IMP_ELIGIBLE_PV'
#         ],
#     )
#
#
# # @pytest.mark.xfail()
# def test_town_stay_expenditure_imputation():
#     # Expected failure on VISIT_WT and EXPENDITURE_WT columns.  For further information see:
#     # https://collaborate2.ons.gov.uk/confluence/display/QSS/Differing+values+between+SAS+and+Python+outputs
#
#     log.info("Testing Calculation 13 --> town_stay_expenditure_imputation")
#     town_stay_expenditure.town_stay_expenditure_imputation_step(run_id)
#
#     survey_output(
#         "TOWN_AND_STAY",
#         "data/calculations/Q3_2017/town_and_stay/town_surveysubsample_2017.csv",
#         [
#             'SERIAL', 'SPEND1', 'SPEND2', 'SPEND3', 'SPEND4', 'SPEND5', 'SPEND6', 'SPEND7', 'SPEND8', 'PURPOSE_PV',
#             'STAYIMPCTRYLEVEL1_PV', 'STAYIMPCTRYLEVEL2_PV', 'STAYIMPCTRYLEVEL3_PV', 'STAYIMPCTRYLEVEL4_PV',
#             'TOWN_IMP_ELIGIBLE_PV'
#         ],
#     )
#
#
# def test_airmiles():
#     log.info("Testing Calculation 14 --> airiles")
#     air_miles.airmiles_step(run_id)
#     survey_output(
#         "AIRMILES",
#         "data/calculations/Q3_2017/stay/air_surveysubsample_2017.csv",
#         [
#             'SERIAL', 'UKLEG', 'OVLEG', 'DIRECTLEG'
#         ]
#     )


def survey_output(test_name, expected_survey_output, survey_output_columns, status=True, false_cols=None):
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

    if not status:
        columns = false_cols

        # Assert columns do not match
        assert_frame_not_equal(survey_results, survey_expected, columns, test_name)

        # Assert remaining dataframe for Spend Imputation matches for accuracy
        survey_results.drop(columns, axis=1, inplace=True)
        survey_expected.drop(columns, axis=1, inplace=True)

    # Test survey outputs
    log.info(f"Testing survey results for {test_name}")
    assert_frame_equal(survey_results, survey_expected, check_dtype=False, check_less_precise=True)


def summary_output(test_name, expected_summary_output, summary_output_table, summary_output_columns):
    # Get summary results
    log.info(f"Testing summary results for {test_name}")

    # Create comparison summary dataframes
    summary_expected = pd.read_csv(expected_summary_output, engine='python')

    if 'RUN_ID' in summary_expected:
        summary_expected.drop('RUN_ID', axis=1, inplace=True)

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
