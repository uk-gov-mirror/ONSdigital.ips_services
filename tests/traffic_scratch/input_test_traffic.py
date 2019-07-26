import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

sas_input = '/Users/ThornE1/PycharmProjects/ips_services/tests/traffic_scratch/sas_survey_traffic_in.csv'
py_input = '/Users/ThornE1/PycharmProjects/ips_services/tests/traffic_scratch/traffic_survey_in.csv'

# sas_output = ''
# py_output = ''

# 'SERIAL'
# 'SAMP_PORT_GRP_PV'
# 'SHIFT_WT'
# 'NON_RESPONSE_WT'
# 'MINS_WT'
# 'TRAFFIC_WT'
# 'ARRIVEDEPART'
# 'PORTROUTE'
#
# 'TRAFFICTOTAL'
# 'TRAFDESIGNWEIGHT'


@pytest.mark.parametrize('col', [
    ('SERIAL'), ('SAMP_PORT_GRP_PV')
])
def test_step_input_cols(col):
    sas_df = pd.read_csv(sas_input, engine='python')
    py_df = pd.read_csv(py_input, engine='python')

    # Sort
    sas_df.sort_values(by='SERIAL', axis=0, inplace=True)
    sas_df.reset_index(drop=True, inplace=True)

    # Sort
    py_df.sort_values(by='SERIAL', axis=0, inplace=True)
    py_df.reset_index(drop=True, inplace=True)

    # # Frobnicate
    # sas_df = sas_df[sas_df['SERIAL'].isin(py_df['SERIAL'])]
    # sas_df.reset_index(drop=True, inplace=True)

    sas_df = sas_df[['SERIAL', col]]
    py_df = py_df[['SERIAL', col]]

    try:
        assert_frame_equal(sas_df, py_df, check_dtype=False)
    except AssertionError:
        sas_df.to_csv(f'/Users/ThornE1/PycharmProjects/ips_services/tests/traffic_scratch/outputs/sas_{col}.csv')
        py_df.to_csv(f'/Users/ThornE1/PycharmProjects/ips_services/tests/traffic_scratch/outputs/py_{col}.csv')
        raise



# @pytest.mark.parametrize('col', [
#     ('SERIAL'), ('VISIT_WT'), ('VISIT_WTK'), ('STAY_WT'), ('STAY_WTK'), ('EXPENDITURE_WT'), ('EXPENDITURE_WTK'),
#     ('NIGHTS1'), ('NIGHTS2'), ('NIGHTS3'), ('NIGHTS4'), ('NIGHTS5'), ('NIGHTS6'), ('NIGHTS7'), ('NIGHTS8'), ('STAY1K'),
#     ('STAY2K'), ('STAY3K'), ('STAY4K'), ('STAY5K'), ('STAY6K'), ('STAY7K'), ('STAY8K'), ('PURPOSE_PV'),
#     ('STAYIMPCTRYLEVEL1_PV'), ('STAYIMPCTRYLEVEL2_PV'), ('STAYIMPCTRYLEVEL3_PV'), ('STAYIMPCTRYLEVEL4_PV'),
#     ('REG_IMP_ELIGIBLE_PV')
# ])
# def test_step_output_cols(col):
#     sas_df = pd.read_csv(sas_output, engine='python')
#     py_df = pd.read_csv(py_output, engine='python')
#
#     # Sort
#     sas_df.sort_values(by='SERIAL', axis=0, inplace=True)
#     sas_df.reset_index(drop=True, inplace=True)
#
#     # Sort
#     py_df.sort_values(by='SERIAL', axis=0, inplace=True)
#     py_df.reset_index(drop=True, inplace=True)
#
#     # # Frobnicate
#     # sas_df = sas_df[sas_df['SERIAL'].isin(py_df['SERIAL'])]
#     # sas_df.reset_index(drop=True, inplace=True)
#
#     sas_df = sas_df[['SERIAL', col]]
#     py_df = py_df[['SERIAL', col]]
#
#     try:
#         assert_frame_equal(sas_df, py_df, check_dtype=False)
#     except AssertionError:
#         sas_df.to_csv(f'/Users/ThornE1/PycharmProjects/ips_services/tests/scratch/outputs/sas_{col}.csv')
#         py_df.to_csv(f'/Users/ThornE1/PycharmProjects/ips_services/tests/scratch/outputs/py_{col}.csv')
#         raise
